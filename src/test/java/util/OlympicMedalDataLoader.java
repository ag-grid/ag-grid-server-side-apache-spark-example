package util;

import com.github.javafaker.Faker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

public class OlympicMedalDataLoader {

    private static Faker faker = new Faker();

    private static String FILENAME = "src/main/resources/data/result.csv";
    private static int BATCH_SIZE = 10_000_000;

    public static void main(String[] args) throws IOException {
        Files.write(Paths.get(FILENAME),
                (Iterable<String>) IntStream
                        .range(0, BATCH_SIZE)
                        .mapToObj(i -> randomResult())::iterator, APPEND);
    }

    private static String randomResult() {
        int gold = faker.number().numberBetween(0, 5);
        int silver = faker.number().numberBetween(0, 5);
        int bronze = faker.number().numberBetween(0, 5);
        int total = gold + silver + bronze;

        return Stream.of(
                faker.name().firstName() + " " + faker.name().lastName(),
                faker.number().numberBetween(18, 45),
                COUNTRIES.get(faker.number().numberBetween(0, COUNTRIES.size() - 1)),
                faker.number().numberBetween(1900, 2017),
                SPORTS.get(faker.number().numberBetween(0, SPORTS.size() - 1)),
                gold,
                silver,
                bronze,
                total
        ).map(Object::toString).collect(joining(","));
    }

    static List<String> SPORTS = asList("Speed Skating", "Cross Country Skiing", "Diving", "Biathlon", "Ski Jumping", "Nordic Combined", "Athletics", "Table Tennis", "Tennis", "Synchronized Swimming", "Rowing", "Equestrian", "Canoeing", "Badminton", "Weightlifting", "Waterpolo", "Triathlon", "Taekwondo", "Softball", "Snowboarding", "Sailing", "Modern Pentathlon", "Ice Hockey", "Hockey", "Football", "Freestyle Skiing", "Curling", "Beach Volleyball", "Swimming", "Gymnastics", "Short-Track Speed Skating", "Cycling", "Alpine Skiing", "Shooting", "Fencing", "Bobsleigh", "Archery", "Wrestling", "Volleyball", "Trampoline", "Skeleton", "Rhythmic Gymnastics", "Luge", "Judo", "Handball", "Figure Skating", "Baseball", "Boxing", "Basketball");
    static List<String> COUNTRIES = asList("United States", "Barbados", "Eritrea", "Mozambique", "Panama", "Russia", "Australia", "Canada", "Norway", "China", "Zimbabwe", "Netherlands", "South Korea", "Croatia", "France", "Japan", "Hungary", "Germany", "Poland", "South Africa", "Sweden", "Ukraine", "Italy", "Czech Republic", "Austria", "Finland", "Romania", "Great Britain", "Jamaica", "Singapore", "Belarus", "Chile", "Spain", "Tunisia", "Brazil", "Slovakia", "Costa Rica", "Bulgaria", "Switzerland", "New Zealand", "Estonia", "Kenya", "Ethiopia", "Trinidad and Tobago", "Turkey", "Morocco", "Bahamas", "Slovenia", "Armenia", "Azerbaijan", "India", "Puerto Rico", "Egypt", "Kazakhstan", "Iran", "Georgia", "Lithuania", "Cuba", "Colombia", "Mongolia", "Uzbekistan", "North Korea", "Tajikistan", "Kyrgyzstan", "Greece", "Macedonia", "Moldova", "Chinese Taipei", "Indonesia", "Thailand", "Vietnam", "Latvia", "Venezuela", "Mexico", "Nigeria", "Qatar", "Serbia", "Serbia and Montenegro", "Hong Kong", "Denmark", "Portugal", "Argentina", "Afghanistan", "Gabon", "Dominican Republic", "Belgium", "Kuwait", "United Arab Emirates", "Cyprus", "Israel", "Algeria", "Montenegro", "Iceland", "Paraguay", "Cameroon", "Saudi Arabia", "Ireland", "Malaysia", "Uruguay", "Togo", "Mauritius", "Syria", "Botswana", "Guatemala", "Bahrain", "Grenada", "Uganda", "Sudan", "Ecuador");
}