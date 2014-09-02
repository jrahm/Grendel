{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
module Grendel where

import Data.Either
import Control.Applicative
import Database.HDBC
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC

import qualified Data.Foldable as Fold
import Data.Convertible

data Sql c a = Sql (c -> IO (Either String a))

class SqlEncodable a where
    sqlEncode :: [SqlValue] -> Either String a

class Selectable a where
    serializeEntity :: a -> String 

data TableSelection = From String
data Selection = All | Column String | Literal SqlValue | Foreign String String 

literal :: (Convertible a SqlValue) => a -> Selection
literal = Literal . toSql

class QuerySerialize a where
    qSerialize :: a -> (ByteString, [SqlValue])

instance QuerySerialize TableSelection where
    qSerialize (From str) = (BC.pack $ "FROM " ++ str,[])
instance QuerySerialize Selection where
    qSerialize All = (BC.pack "*",[])
    qSerialize (Column l) = (BC.pack l,[])
    qSerialize (Literal s) = (BC.pack "?", [s])
    qSerialize (Foreign a b) = (BC.pack $ a ++ "." ++ b, [])


data PreparedStatement = PreparedStatement ByteString [ SqlValue ] deriving Show

class SelectReturn a where
    apply :: (ByteString,[SqlValue]) -> a
class SqlModifier a where
    serialize :: a -> (ByteString,[SqlValue])
instance SelectReturn PreparedStatement where
    apply = uncurry PreparedStatement
instance SqlModifier ByteString where
    serialize = (,[])
instance SqlModifier WhereClause where
    serialize (Where s) = 
        let (str, lst) = qSerialize s in (Fold.fold ["WHERE ", str], lst)

data Condition = Equals Selection Selection | Or Condition Condition | And Condition Condition
instance QuerySerialize Condition where
    qSerialize (Equals a b) = Fold.fold [qSerialize a, ("=",[]), qSerialize b]
    qSerialize (Or a b)  = Fold.fold [("(",[]), qSerialize a, (") OR (",[]), qSerialize b, (")",[])]
    qSerialize (And a b) = Fold.fold [("(",[]), qSerialize a, (") AND (",[]), qSerialize b, (")",[])]

(^==) :: Selection -> Selection -> Condition
(^==) = Equals

(^&&) :: Condition -> Condition -> Condition
(^&&) = And

(^||) :: Condition -> Condition -> Condition
(^||) = Or

(^.) :: String -> String -> Selection
(^.) = Foreign

infixl 4 ^. 
infixl 3 ^==
infixl 2 ^&&
infixl 2 ^||


data WhereClause = Where Condition
data JoinClause = Join String Selection Selection

instance SqlModifier JoinClause where
    serialize (Join a b c) = 
        let (b', v1) = qSerialize b
            (c', v2) = qSerialize c in
        (Fold.fold [" JOIN ", BC.pack a, " ON ", b', "=", c'], v1 ++ v2)

instance (SelectReturn r, SqlModifier a) => SelectReturn (a -> r) where
    apply bs a = apply (Fold.fold [bs, serialize a])

select :: (SelectReturn r) => Selection -> TableSelection -> r
select s t = apply $ Fold.fold [("SELECT ",[]), qSerialize s, (" ",[]), qSerialize t, (" ",[])]

on :: String -> Selection -> Selection -> JoinClause
on = Join

main :: IO()
main  = let q = select All (From "articles")
                        (Where $ "user"^."name" ^== Column "author" ^&&
                                Column "date" ^== (literal ("this \" Is a _ String"::String)))
                        (Join "email" ("user"^."id") ("email"^."user_id"))
            in (putStrLn . show) (q :: PreparedStatement)

build :: (IConnection c, SqlEncodable a) => PreparedStatement -> Sql c [a]
build (PreparedStatement str vals) = Sql $ \conn -> do 
        stmt <- prepare conn (BC.unpack str)
        _ <- execute stmt vals 
        (as,bs) <- (partitionEithers . map sqlEncode) <$> fetchAllRows stmt;
        if null as then return $ Right bs else return $ Left $ head as

instance (IConnection c) => Functor (Sql c) where
    fmap f1 (Sql f2) = Sql (\c -> fmap f1 <$> f2 c)

runSql :: (IConnection c) => c -> Sql c a -> IO (Either String a)
runSql c (Sql f) = f c

-- user <- selectSingle All (From "users") (Where (Column "name") .==. (Literal "John"))
-- articles <- select All (From "articles") (Where (Clumn "author") .==. (Foregin user "id")
