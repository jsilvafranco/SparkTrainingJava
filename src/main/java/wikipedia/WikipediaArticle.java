package wikipedia;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Stream;

public class WikipediaArticle implements Serializable{
    private String title;
    private String text;

    public WikipediaArticle(String title, String text) {
        this.title = title;
        this.text = text;
    }
    /**
     * @return Whether the text of this article mentions `lang` or not
     * @param lang Language to look for (e.g. "Scala")
     */
    public Boolean mentionsLanguage(String lang){
        return text.contains(lang);
    }

    public Stream<String> languages(Stream<String> lang){
        return lang.map(l -> {
            if(mentionsLanguage(l))
                return l;
            else return null;

        }).filter(Objects::nonNull);

    }

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }
}
