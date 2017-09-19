package wikipedia;

import java.io.File;
import java.net.URL;

class WikipediaData {

  String filePath() {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("wikipedia.dat");
    if (resource == null)
      return null;
    return new File(resource.getFile()).getPath();
  }

  WikipediaArticle parse(String line) {
    String subs = "</title><text>";
    int i = line.indexOf(subs);
    String title = line.substring(14, i);
    String text  = line.substring(i + subs.length(), line.length()-16);
    return new WikipediaArticle(title, text);
  }
}
