package unit;

import com.github.javafaker.Faker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

public class Fred {

    static Faker faker = new Faker();

    public static void main(String[] args) throws IOException {

        String filename = "/Users/robertclarke/Development/projects/scratch/olympic-medals/src/main/resources/data/result1.csv";

        Files.write(Paths.get(filename),
                (Iterable<String>) IntStream
                        .range(0, 10_000_000)
                        .mapToObj(i -> randomResult())::iterator, APPEND);
    }

    private static String randomResult() {
        return Stream.of(
                faker.name().firstName() + " " + faker.name().lastName(),
                faker.number().numberBetween(18, 45),
                COUNTRIES.get(faker.number().numberBetween(0, COUNTRIES.size() - 1)),
                faker.number().numberBetween(1900, 2017),
                "24/08/2008",
                SPORTS.get(faker.number().numberBetween(0, SPORTS.size() - 1)),
                faker.number().numberBetween(0, 5),
                faker.number().numberBetween(0, 5),
                faker.number().numberBetween(0, 5),
                faker.number().numberBetween(0, 5)
        ).map(Object::toString).collect(joining(","));
    }

    static List<String> SPORTS = asList("Speed Skating", "Cross Country Skiing", "Diving", "Biathlon", "Ski Jumping", "Nordic Combined", "Athletics", "Table Tennis", "Tennis", "Synchronized Swimming", "Rowing", "Equestrian", "Canoeing", "Badminton", "Weightlifting", "Waterpolo", "Triathlon", "Taekwondo", "Softball", "Snowboarding", "Sailing", "Modern Pentathlon", "Ice Hockey", "Hockey", "Football", "Freestyle Skiing", "Curling", "Beach Volleyball", "Swimming", "Gymnastics", "Short-Track Speed Skating", "Cycling", "Alpine Skiing", "Shooting", "Fencing", "Bobsleigh", "Archery", "Wrestling", "Volleyball", "Trampoline", "Skeleton", "Rhythmic Gymnastics", "Luge", "Judo", "Handball", "Figure Skating", "Baseball", "Boxing", "Basketball");
    static List<String> COUNTRIES = asList("United States", "Russia", "Australia", "Canada", "Norway", "China", "Zimbabwe", "Netherlands", "South Korea", "Croatia", "France", "Japan", "Hungary", "Germany", "Poland", "South Africa", "Sweden", "Ukraine", "Italy", "Czech Republic", "Austria", "Finland", "Romania", "Great Britain", "Jamaica", "Singapore", "Belarus", "Chile", "Spain", "Tunisia", "Brazil", "Slovakia", "Costa Rica", "Bulgaria", "Switzerland", "New Zealand", "Estonia", "Kenya", "Ethiopia", "Trinidad and Tobago", "Turkey", "Morocco", "Bahamas", "Slovenia", "Armenia", "Azerbaijan", "India", "Puerto Rico", "Egypt", "Kazakhstan", "Iran", "Georgia", "Lithuania", "Cuba", "Colombia", "Mongolia", "Uzbekistan", "North Korea", "Tajikistan", "Kyrgyzstan", "Greece", "Macedonia", "Moldova", "Chinese Taipei", "Indonesia", "Thailand", "Vietnam", "Latvia", "Venezuela", "Mexico", "Nigeria", "Qatar", "Serbia", "Serbia and Montenegro", "Hong Kong", "Denmark", "Portugal", "Argentina", "Afghanistan", "Gabon", "Dominican Republic", "Belgium", "Kuwait", "United Arab Emirates", "Cyprus", "Israel", "Algeria", "Montenegro", "Iceland", "Paraguay", "Cameroon", "Saudi Arabia", "Ireland", "Malaysia", "Uruguay", "Togo", "Mauritius", "Syria", "Botswana", "Guatemala", "Bahrain", "Grenada", "Uganda", "Sudan", "Ecuador", "Panama", "Eritrea", "Sri Lanka", "Mozambique", "Barbados");
}