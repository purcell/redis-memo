(ns redis-memo
  (:require redis)
  (:import (java.net URLEncoder)))

;; --------------------------------------------------------------------------------
;; Default connection params
;; --------------------------------------------------------------------------------

(def memo-server {:host "localhost" :port 6379 :db 14})


;; --------------------------------------------------------------------------------
;; Helpers
;; --------------------------------------------------------------------------------

(defn- encode-value
  "Encode an object so that it is safe to pass to Redis, which complains about key
strings containing spaces etc. (redis-clojure doesn't escape keys)"
  [value]
  (with-bindings [*print-level* nil]
    (pr-str value)))

(defn- decode-value [value]
  (with-in-str value (read)))

(defn- encode-key
  "Encode an object so that it is safe to use as a Redis key,
   avoiding invalid command strings containing spaces."
  [key]
  (URLEncoder/encode (encode-value key)))

;; --------------------------------------------------------------------------------
;; Public API
;; --------------------------------------------------------------------------------

(defmacro with-memo-server
  "Executes forms with a redis connection to memo-server."
  [& forms]
  `(redis/with-server memo-server ~@forms))

(defn memoize-with-redis
  "Returns a memoized version of a referentially transparent function. The
  memoized version of the function uses Redis to keep a cache of the
  mapping from arguments to results.

  Unlike clojure.core/memoize, this explicitly shortcuts calling the original
  function when a nil result has previously been memoized."
  [keyprefix f]
  (fn [& args]
    (with-memo-server
      (let [k (encode-key (cons keyprefix args))]
        (if-let [v (redis/get k)]
          (decode-value v)
          (let [ret (apply f args)]
            (redis/set k (encode-value ret))
            ret))))))

(defmacro defn-redis-memo
  "Just like defn, but memoizes the function using memoize-with-redis"
  [fn-name & defn-stuff]
  `(do
     (defn ~fn-name ~@defn-stuff)
     (alter-var-root (var ~fn-name) (partial memoize-with-redis (str (var ~fn-name))))
     (var ~fn-name)))
