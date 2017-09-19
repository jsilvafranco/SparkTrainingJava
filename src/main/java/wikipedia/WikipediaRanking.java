package wikipedia;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WikipediaRanking {

  static Stream<String> langs = Arrays.asList(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy"
  ).stream();


  //Init SparkConf
  public static SparkConf conf  = null;
  public static JavaSparkContext sc = null;
  static WikipediaData wikipediaData = new WikipediaData();
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  static JavaRDD<WikipediaArticle> wikiRdd = null;

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  public static Long occurrencesOfLang(String lang, JavaRDD<WikipediaArticle> rdd){
      return 0L;
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static List<Tuple2<String, Long>> rankLangs(Stream<String> langs , JavaRDD<WikipediaArticle> rdd){
    return null;
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  public static JavaRDD<Tuple2<String, List<WikipediaArticle>>> makeIndex(Stream<String> langList, JavaRDD<WikipediaArticle> rdd){
    return null;
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static List<Tuple2<String, Long>> rankLangsUsingIndex(JavaRDD<Tuple2<String, List<WikipediaArticle>>> index){
          return null;
    }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static Stream<Tuple2<String, Long>>  rankLangsReduceByKey( Stream<String> langs, JavaRDD<WikipediaArticle> rdd ){
      return null;
  }

  public static void main(String[] args ) {

    /* Languages ranked according to (1) */
    List<Tuple2<String, Long>> langsRanked = rankLangs(langs, wikiRdd);

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    JavaRDD<Tuple2<String, List<WikipediaArticle>>> index = makeIndex(langs, wikiRdd);

    /* Languages ranked according to (2), using the inverted index */
    List<Tuple2<String, Long>> langsRanked2 = rankLangsUsingIndex(index);

    /* Languages ranked according to (3) */
    Stream<Tuple2<String, Long>>  langsRanked3 = rankLangsReduceByKey(langs, wikiRdd);


    sc.stop();
  }

}
