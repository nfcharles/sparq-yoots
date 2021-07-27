(defproject nfcharles/sparq-yoots "0.3.1"
  :description "Spark configuration utilities"
  :url "https://github.com/nfcharles/sparq-yoots"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/timbre "4.10.0"]]
  :plugins [[lein-cloverage "1.0.13"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]]
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.8.0"]
                                       [org.apache.spark/spark-core_2.11 "2.2.3" :exclusions [commons-codec]]
                                       [org.apache.spark/spark-sql_2.11 "2.2.3" :exclusions [commons-codec]]]}
             :uberjar {:aot :all
                       :dependencies [[org.clojure/clojure "1.8.0"]
                                      [org.apache.spark/spark-core_2.11 "2.2.3" :exclusions [commons-codec]]
                                      [org.apache.spark/spark-sql_2.11 "2.2.3" :exclusions [commons-codec]]]}
             :dev {:aot :all
                   :dependencies [[org.clojure/clojure "1.8.0"]
                                  [org.apache.spark/spark-core_2.11 "2.2.3" :exclusions [commons-codec]]
                                  [org.apache.spark/spark-sql_2.11 "2.2.3" :exclusions [commons-codec]]]}}
  :aot :all
  :target-path "target/%s/"
  :deploy-repositories [["releases" :clojars]]
  :aliases {"update-readme-version" ["shell" "sed" "-i" "s/\\\\[sparq-yoots \"[0-9.]*\"\\\\]/[sparq-yoots \"${:version}\"]/" "README.md"]}
  :java-source-paths ["src/main/java"]
  :source-paths ["src/main/clojure"])

(comment
  :release-tasks [["shell" "git" "diff" "--exit-code"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["changelog" "release"]
                  ["update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["deploy"]
                  ["vcs" "push"]])
