package org.example;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;

public class Main {
    public static void main(String[] args) {
        // THIS IS IN CASE YOU WANT TO WRITE THE LOGS TO THE LOCAL FILE
//            String localOutputFile = "src/main/resources/result.txt";
//
//            PrintStream out = new PrintStream(new File(localOutputFile));
//            System.setOut(out); // This redirects System.out to the output file

        SparkSession sparkSession = SparkSession.builder()
                .appName("IMDB Spark Cluster 1")
                .master("yarn")
                .config("spark.submit.deployMode", "cluster")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .getOrCreate();

        // S3 input paths
        String basicsPath = "s3a://cs-bucket-301/input/title.basics.tsv";
        String ratingsPath = "s3a://cs-bucket-301/input/title.ratings.tsv";

        // Define the S3 output path
        String s3resultPath = "s3a://cs-bucket-301/output/result.txt";

        String result = "";
        try {
            JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

            result += "SOLUTION: 1\n";
            result = totalTvSeries(sc, basicsPath, result);
            result += "\n";

            result += "SOLUTION: 2\n";
            result = runtimeCalc(sc, basicsPath, result);
            result += "\n";

            result += "SOLUTION: 3\n";
            result = avgRuntimeByYear(sc, basicsPath, result);
            result += "\n";

            result += "SOLUTION: 4\n";
            result = avgRatingByGenre(sc, basicsPath, ratingsPath, result);
            result += "\n";

            // upload the output file to S3
            writeToS3(s3resultPath, result);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String totalTvSeries(JavaSparkContext sc, String filePath, String result) {
        JavaRDD<String> lines = sc.textFile(filePath);
        long totalTvSeries = lines.filter(line -> !line.startsWith("tconst"))
                .map(line -> line.split("\\t"))
                .filter(columns -> columns.length > 1 && "tvSeries".equals(columns[1]))
                .count();
        result += "Total Number of tvSeries: " + totalTvSeries + "\n\n";

        return result;
    }

    private static String runtimeCalc(JavaSparkContext sc, String filePath, String result) {
        JavaRDD<String> lines = sc.textFile(filePath);
        JavaDoubleRDD validMovies = lines.map(line -> line.split("\t"))
                .filter(column -> column.length > 7 && "movie".equals(column[1]))
                .mapToDouble(column -> {
                    try {
                        String runtimeStr = column[7];
                        return !Objects.equals(runtimeStr, "\\N") ? Double.parseDouble(runtimeStr) : Double.NaN;
                    } catch (NumberFormatException e) {
                        return Double.NaN;
                    }
                }).filter(runtime -> runtime > 0);

        result += "Minimum Runtime: " + validMovies.min() + "\n";
        result += "Maximum Runtime: " + validMovies.max() + "\n";
        result += "Average Runtime: " + validMovies.mean() + "\n";

        return result;
    }

    private static String avgRuntimeByYear(JavaSparkContext sc, String filePath, String result) {
        JavaRDD<String> lines = sc.textFile(filePath);
        JavaRDD<String[]> validMovies = lines.map(line -> line.split("\t"))
                .filter(column -> {
                    if(column.length > 8 && "movie".equals(column[1]) && !Objects.equals(column[5], "\\N") && !Objects.equals(column[7], "\\N")) {
                        try {
                            int year = Integer.parseInt(column[5]);
                            return year >= 1990 && year < 2000;
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            return false;
                        }
                    }
                    return false;
                });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> yearDurationPair = pairYearAndDuration(validMovies);

        JavaPairRDD<Integer, Double> yearAndAverage = yearDurationPair
                .mapValues(value -> ((double)value._1 / value._2))
                .sortByKey();

        for (Tuple2<Integer, Double> entry : yearAndAverage.collect()) {
            result += String.format("Year %d: %.3f minutes%n\n", entry._1, entry._2);
        }
        return result;
    }

    private static JavaPairRDD<Integer, Tuple2<Integer, Integer>> pairYearAndDuration(JavaRDD<String[]> validMovies) {
        return validMovies.mapToPair(column -> {
            try {
                int year = Integer.parseInt(column[5]);
                int duration = Integer.parseInt(column[7]);
                return new Tuple2<>(year, new Tuple2<>(duration, 1));
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
                return null;  // return null on failure
            }
        }).reduceByKey((a, b) ->
                new Tuple2<>(a._1 + b._1, a._2 + b._2)  // here _1 indicates the duration and _2 is count
        );
    }

    private static String avgRatingByGenre(JavaSparkContext sc, String basicsPath, String ratingsPath, String result) {
        JavaRDD<String> basicsRDD = sc.textFile(basicsPath);
        JavaRDD<String> ratingsRDD = sc.textFile(ratingsPath);

        // this will map tconst with its corresponding rating
        JavaPairRDD<String, Double> ratingMap = ratingsRDD.filter(line -> !line.startsWith("tconst"))
                .map(line -> line.split("\t"))
                .mapToPair(column -> {
                    try {
                        String tconst = column[0];
                        Double rating = Double.parseDouble(column[1]);
                        return new Tuple2<>(tconst, rating);
                    } catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                        return null;
                    }
                });

        // this will map tconst with its corresponding genres
        JavaPairRDD<String, String[]> genreMap = basicsRDD.filter(line -> !line.startsWith("tconst"))
                .map(column -> column.split("\t"))
                .filter(column -> column[1].equals("movie"))
                .mapToPair(column -> {
                    try {
                        String tconst = column[0];
                        String[] genres = column[8].split(",");
                        return new Tuple2<>(tconst, genres);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        return null;
                    }
                });

        // joining the genreMap and ratingMap
        JavaPairRDD<String, Tuple2<String[], Double>> joinedRDD = genreMap.join(ratingMap);

        JavaPairRDD<String, Double> genreRatingRDD = joinedRDD.flatMapToPair(entry -> {
            String[] genres = entry._2._1;
            Double rating = entry._2._2;

            return Arrays.stream(genres)
                    .filter(genre -> !genre.equals("\\N"))
                    .map(genre -> new Tuple2<>(genre, rating))
                    .iterator();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> genreStatsRDD = genreRatingRDD.mapValues(value -> new Tuple2<>(value, 1))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Double> resultRDD = genreStatsRDD
                .mapValues(value -> ((double)value._1/value._2))
                .sortByKey();

        for (Tuple2<String, Double> entry : resultRDD.collect()) {
            result += String.format("%s : %.3f%n\n", entry._1, entry._2);
        }
        return result;
    }

    private static void writeToS3(String s3resultPath, String result) throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(new java.net.URI(s3resultPath), new org.apache.hadoop.conf.Configuration());

        // Create an output stream to the S3 path
        Path path = new Path(s3resultPath);
        if (fs.exists(path)) {
            fs.delete(path, true);  // Delete if it already exists
        }

        try (FSDataOutputStream outputStream = fs.create(path)) {
            outputStream.write(result.getBytes());
        } finally {
            fs.close();
        }
    }
}
