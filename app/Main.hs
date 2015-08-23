{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards     #-}

module Main where

import           Control.Foldl        (Fold(..))
import qualified Control.Foldl        as L
import           Control.Lens         (_Just, _Nothing)
import           Control.Monad
import           Control.Monad.State
import           Data.Csv             (FromRecord, ToRecord)
import           Data.Csv.Streaming
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS
import           Data.Text            (Text)
import qualified Data.Text            as T
import           Data.Vector          (Vector)
import           GHC.Generics         (Generic)
import           Pipes
import qualified Pipes.Prelude        as P

data Session = Session
    { sessionId      :: !Text
    , sessionPage    :: !(Maybe Text)
    , sessionLatency :: !Int
    , sessionTime    :: !(Maybe Double)
    } deriving (Show, Generic)

data Metrics = Metrics
    -- Non-null counts
    { sessionIdCount        :: !Int
    , sessionPageCount      :: !Int
    , sessionLatencyCount   :: !Int
    , sessionTimeCount      :: !Int
    -- Null counts
    , sessionPageNullCount  :: !Int
    , sessionTimeNullCount  :: !Int
    -- Minimums
    , sessionLatencyMinimum :: !(Maybe Int)
    , sessionTimeMinimum    :: !(Maybe Double)
    -- Maximums
    , sessionLatencyMaximum :: !(Maybe Int)
    , sessionTimeMaximum    :: !(Maybe Double)
    -- Avg. text len
    , sessionIdAvgLen       :: !Double
    , sessionPageAvgLen     :: !Double
    }
    -- , session
  deriving Show

instance FromRecord Session
instance ToRecord Session

main :: IO ()
main = do
    bs <- BS.readFile "data.txt"
    L.purely P.fold metricsFold (decode' HasHeader bs) >>= print

decode' :: HasHeader -> ByteString -> Producer Session IO ()
decode' h bs = go (decode h bs)
  where
    go :: Records Session -> Producer Session IO ()
    go (Cons x xs) =
        case x of
            Left s -> do
                err s
                go xs
            Right x' -> do
                yield x'
                go xs
    go (Nil (Just s) _) = err s
    go _ = pure ()

    err :: String -> Producer Session IO ()
    err = liftIO . putStrLn . ("Decoding error: " ++)

metricsFold :: Fold Session Metrics
metricsFold = Metrics
    <$> L.length
    <*> L.premap sessionPage (L.handles _Just L.length)
    <*> L.length
    <*> L.premap sessionTime (L.handles _Just L.length)
    <*> L.premap sessionPage (L.handles _Nothing L.length)
    <*> L.premap sessionTime (L.handles _Nothing L.length)
    <*> L.premap sessionLatency L.minimum
    <*> L.premap sessionTime (L.handles _Just L.minimum)
    <*> L.premap sessionLatency L.maximum
    <*> L.premap sessionTime (L.handles _Just L.maximum)
    <*> avg (L.premap sessionId textLen)
    <*> avg (L.premap sessionPage (L.handles _Just textLen))
  where
    avg :: Fold a Double -> Fold a Double
    avg f = (/) <$> f <*> L.genericLength

    textLen :: Fold Text Double
    textLen = Fold (\(!n) t -> n + T.length t) 0 fromIntegral
      where
        step :: Int -> Text -> Int
        step !n = (n +) . T.length
