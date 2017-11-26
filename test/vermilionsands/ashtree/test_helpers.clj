(ns vermilionsands.ashtree.test-helpers)

(defn less-than-10 [x]
  (< x 10))

(defn less-than-4 [x]
  (< x 4))

(defn watch-and-store []
  (let [a (atom [])]
    [(fn [_ _ old-val new-val]
       (swap! a conj [old-val new-val]))
     a]))

(def watch-log (atom []))

(defn store-to-atom-watch [_ _ old-val new-val]
  (swap! watch-log conj [old-val new-val]))