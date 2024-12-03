# IMDBSpark Application

This project demonstrates the use of Apache Spark and Hadoop to process and analyze IMDB datasets. The application performs various operations such as filtering, aggregation, and calculation of statistics for movie and TV series data stored on AWS S3.

## Features

1. **Total TV Series Count**: Calculates the total number of TV series.
2. **Runtime Statistics for Movies**: Finds the minimum, maximum, and average runtime for movies.
3. **Average Runtime by Year (1990-1999)**: Computes the average runtime of movies for each year in the 1990s.
4. **Average Rating by Genre**: Determines the average rating for movies by genre.

## Prerequisites

To run this project, you need the following:

1. **Java Development Kit (JDK)**: Version 11 or later.
2. **Apache Maven**: To manage dependencies and build the project.
3. **Apache Spark**: Version 3.5.1.
4. **Hadoop (with Yarn)**: Version 3.3.6 or compatible.
5. **AWS S3 Access**: Proper AWS credentials configured for accessing S3 buckets.
6. **Cluster Environment**: Spark and Hadoop cluster setup with Yarn.

## Dataset

- **Title Basics**: `title.basics.tsv` containing metadata like title type, genres, and runtime.
- **Title Ratings**: `title.ratings.tsv` containing user ratings for titles.

These files should be stored in an AWS S3 bucket accessible to the application.

## Project Structure

- **Main.java**: Entry point of the application.
- **Pom.xml**: Maven configuration file for managing dependencies.

## Dependencies

The key dependencies used in this project are:

- **Apache Spark Core**: For distributed data processing.
- **Apache Spark SQL**: For querying structured data.
- **Apache Hadoop Common**: For integration with Hadoop.
- **Hadoop Yarn Client**: For deploying on Yarn clusters.

Refer to the `pom.xml` file for a complete list of dependencies.

## How to Build and Run

### 1. Clone the Repository
```bash
git clone https://github.com/prashantjerk/IMDBSpark.git
cd IMDBSpark
```

### 2. Configure AWS Credentials
Ensure AWS credentials are properly configured on the system running this application.

### 3. Build the Project
```bash
mvn clean package
```

### 4. Run the Application
Submit the Spark job using the following command:
```bash
spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode cluster \
  target/IMDBSpark-1.0-SNAPSHOT.jar
```

### 5. Output

Results will be written to the specified S3 output path (e.g., `s3a://cs-bucket-301/output/result.txt`).

## Functions Overview

### `totalTvSeries`
Counts the number of TV series by filtering `title.basics.tsv`.

### `runtimeCalc`
Calculates the minimum, maximum, and average runtime for movies.

### `avgRuntimeByYear`
Computes the average runtime for movies by year in the 1990s.

### `avgRatingByGenre`
Finds the average rating for each genre by joining `title.basics.tsv` and `title.ratings.tsv`.

### `writeToS3`
Writes the results to an S3 bucket.

## Error Handling
- Logs are printed to the console for debugging.
- Data validation ensures only valid records are processed.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgements
- **Apache Spark** for distributed data processing.
- **AWS S3** for cloud-based storage.
- **IMDB Datasets** for the data used in this project.

## Author
[Prashant Karki](https://github.com/prashantjerk)
