# BDA PRACTICALS — COMPLETE STEP-BY-STEP GUIDE

This single README contains the execution steps and source code for Practical 2c, 3c and 4c.
Save this file as `README.md` in your VS Code project and upload it to GitHub.

> IMPORTANT
> - Practicals 2c and 3c run in the college-configured Ubuntu/WSL Hadoop environment.
> - Practical 4c is performed in Tableau on Windows.
> - The uploaded 2c guide specifies a pipe-separated log format. A matching sample log is included below so the Java program can be tested without guessing the format.
> - The 3c guide specifies Hadoop 3.5.0, Java 17 and Hive 4.1.x. Use the college environment that has these tools configured. Do not randomly install a different Hadoop/Hive version during the practical.
> - Change only the GitHub repository URL and the Windows source path when you upload/clone. The scripts below use your Ubuntu home directory automatically rather than a fixed personal username.

---

# PRACTICAL 2c — USER LOGIN DURATION USING JAVA MAPREDUCE

## What this practical does

1. Reads application log records from HDFS.
2. Mapper extracts successful LOGIN and LOGOUT events for each user.
3. Reducer pairs each LOGIN with the next LOGOUT and totals the session minutes.
4. A second MapReduce job finds the username with the maximum total login duration.

## 2c — Step 1: Open Ubuntu and check the software

Open Ubuntu from the Windows Start menu, then run:

```bash
java -version
javac -version
hadoop version
```

The uploaded practical guide uses Hadoop MapReduce. Continue only if Hadoop is already configured in your college environment.

## 2c — Step 2: Start Hadoop

```bash
start-dfs.sh
start-yarn.sh
jps
```

You should see Hadoop daemon processes. If the start commands are not found, use the college-configured Hadoop machine/WSL setup.

## 2c — Step 3: Create a working folder and sample log

```bash
mkdir -p ~/BDA-Practicals/2c
cd ~/BDA-Practicals/2c

cat > sample.log <<'EOF'
2026-11-17 09:00:00 | INFO | app | user=alice | event=LOGIN | status=SUCCESS
2026-11-17 11:00:00 | INFO | app | user=bob | event=LOGIN | status=SUCCESS
2026-11-17 12:00:00 | INFO | app | user=alice | event=LOGOUT | status=SUCCESS
2026-11-17 13:00:00 | INFO | app | user=alice | event=LOGIN | status=SUCCESS
2026-11-17 14:00:00 | INFO | app | user=bob | event=LOGOUT | status=SUCCESS
2026-11-17 15:00:00 | INFO | app | user=alice | event=LOGOUT | status=SUCCESS
EOF

cat sample.log
```

The sample format is:
`timestamp | level | application | user=... | event=... | status=...`

If your college provides its own `sample.log`, use that file only if it follows the same format.

## 2c — Step 4: Create the Java MapReduce source file

```bash
cat > LoginDuration.java <<'EOF'
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LoginDuration {

    // Format of date and time in the log file
    static DateTimeFormatter format =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // MAPPER: Extract username, timestamp and login/logout event
    public static class LoginMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split("\\s*\\|\\s*");

            if (fields.length < 6) return;

            String time = fields[0];
            String user = "";
            String event = "";
            String status = "";

            // Read required information from each log record
            for (String field : fields) {
                if (field.startsWith("user="))
                    user = field.substring(5);

                if (field.startsWith("event="))
                    event = field.substring(6);

                if (field.startsWith("status="))
                    status = field.substring(7);
            }

            // Accept only successful LOGIN and LOGOUT events
            if (!user.isEmpty() && status.equals("SUCCESS")
                && (event.equals("LOGIN") || event.equals("LOGOUT"))) {

                context.write(new Text(user),
                    new Text(time + "|" + event));
            }
        }
    }

    // REDUCER: Calculate total login time of each user
    public static class LoginReducer
        extends Reducer<Text, Text, Text, LongWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            List<String> records = new ArrayList<>();

            for (Text value : values)
                records.add(value.toString());

            // Sort events according to timestamp
            Collections.sort(records);

            LocalDateTime login = null;
            long total = 0;

            for (String record : records) {
                String[] parts = record.split("\\|");

                LocalDateTime time =
                    LocalDateTime.parse(parts[0], format);

                // Store login time
                if (parts[1].equals("LOGIN")) {
                    login = time;
                }

                // Calculate duration when logout occurs
                else if (parts[1].equals("LOGOUT") && login != null) {
                    long minutes =
                        Duration.between(login, time).toMinutes();

                    if (minutes > 0)
                        total += minutes;

                    login = null;
                }
            }

            // Output username and total login duration
            context.write(key, new LongWritable(total));
        }
    }

    // MAPPER 2: Send all user durations with the same key
    public static class MaxMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");

            if (parts.length == 2) {
                context.write(new Text("MAX"),
                    new Text(parts[0] + "|" + parts[1]));
            }
        }
    }

    // REDUCER 2: Find the user with maximum login duration
    public static class MaxReducer
        extends Reducer<Text, Text, Text, LongWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            String maxUser = "";
            long maxTime = -1;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");

                long time = Long.parseLong(parts[1]);

                if (time > maxTime) {
                    maxTime = time;
                    maxUser = parts[0];
                }
            }

            // Output maximum user's name and duration
            context.write(new Text(maxUser), new LongWritable(maxTime));
        }
    }

    // MAIN: Configure and execute both MapReduce jobs
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // JOB 1: Calculate login duration for each user
        Job job1 = Job.getInstance(conf, "Login Duration");

        job1.setJarByClass(LoginDuration.class);
        job1.setMapperClass(LoginMapper.class);
        job1.setReducerClass(LoginReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true))
            System.exit(1);

        // JOB 2: Find maximum login duration
        Job job2 = Job.getInstance(conf, "Maximum Login Duration");

        job2.setJarByClass(LoginDuration.class);
        job2.setMapperClass(MaxMapper.class);
        job2.setReducerClass(MaxReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
EOF
```

## 2c — Step 5: Compile the Java program and create the JAR

```bash
mkdir -p build

javac -cp "$(hadoop classpath)" -d build LoginDuration.java

jar -cvf LoginDuration.jar -C build .

ls -lh LoginDuration.jar
```

## 2c — Step 6: Upload the input file to HDFS

```bash
hdfs dfs -mkdir -p /user/$USER/login_project/input

hdfs dfs -put -f sample.log \
/user/$USER/login_project/input/

hdfs dfs -ls /user/$USER/login_project/input/
hdfs dfs -cat /user/$USER/login_project/input/sample.log
```

## 2c — Step 7: Remove old output folders and run both jobs

Hadoop requires the output folders not to exist before a job starts.

```bash
hdfs dfs -rm -r -f /user/$USER/login_project/output1
hdfs dfs -rm -r -f /user/$USER/login_project/output2

hadoop jar LoginDuration.jar LoginDuration \
/user/$USER/login_project/input \
/user/$USER/login_project/output1 \
/user/$USER/login_project/output2
```

## 2c — Step 8: Display the results

```bash
echo "TOTAL LOGIN DURATION FOR EACH USER:"
hdfs dfs -cat /user/$USER/login_project/output1/part-r-00000

echo "USER WITH MAXIMUM LOGIN DURATION:"
hdfs dfs -cat /user/$USER/login_project/output2/part-r-00000
```

Expected result for the included sample data:

```text
alice    300
bob      180
```

The maximum-duration output should identify:

```text
alice    300
```

The duration is measured in minutes. This example assumes each user's successful LOGIN is followed by a successful LOGOUT. An unmatched login is ignored.

## 2c — Step 9: Stop Hadoop after finishing

```bash
stop-yarn.sh
stop-dfs.sh
```

---

# PRACTICAL 3c — FOREST FIRE ANALYSIS USING PYTHON MAPREDUCE AND HIVE

## What this practical does

Part A uses Python Hadoop Streaming to calculate, for each month:
- Number of fire records
- Average temperature
- Total burned area
- Largest individual burned area

Part B uses HiveQL to query the Forest Fires dataset.

## 3c — PART A: PYTHON MAPREDUCE

### 3c — Step 1: Open Ubuntu and check software

```bash
java -version
hadoop version
python3 --version
```

### 3c — Step 2: Start Hadoop

```bash
start-dfs.sh
start-yarn.sh
jps
```

### 3c — Step 3: Create the project directory and download the dataset

```bash
mkdir -p ~/BDA-Practicals/3c
cd ~/BDA-Practicals/3c

curl -L -o forestfires.csv \
https://archive.ics.uci.edu/ml/machine-learning-databases/forest-fires/forestfires.csv

head -5 forestfires.csv
wc -l forestfires.csv
```

### 3c — Step 4: Create the Python mapper

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3

import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if not row or row[0].strip().lower() == "x":
        continue

    try:
        month = row[2].strip()
        temperature = float(row[8])
        area = float(row[12])

        print(f"{month}\t1,{temperature},{area}")

    except (ValueError, IndexError):
        continue
EOF
```

### 3c — Step 5: Create the Python reducer

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3

import sys

current_month = None
fire_count = 0
temperature_sum = 0.0
area_sum = 0.0
max_area = 0.0


def output_result(month, count, temp_sum, burned_area, largest_fire):
    if month is not None and count > 0:
        average_temp = temp_sum / count
        print(
            f"{month}\t{count}\t{average_temp:.2f}\t"
            f"{burned_area:.2f}\t{largest_fire:.2f}"
        )


for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    try:
        month, values = line.split("\t", 1)
        count_value, temp_value, area_value = values.split(",")

        count_value = int(count_value)
        temp_value = float(temp_value)
        area_value = float(area_value)

    except ValueError:
        continue

    if current_month != month:
        output_result(
            current_month,
            fire_count,
            temperature_sum,
            area_sum,
            max_area
        )

        current_month = month
        fire_count = 0
        temperature_sum = 0.0
        area_sum = 0.0
        max_area = 0.0

    fire_count += count_value
    temperature_sum += temp_value
    area_sum += area_value
    max_area = max(max_area, area_value)


output_result(
    current_month,
    fire_count,
    temperature_sum,
    area_sum,
    max_area
)
EOF
```

### 3c — Step 6: Give permission and test locally

```bash
chmod +x mapper.py reducer.py

cat forestfires.csv | python3 mapper.py | sort | python3 reducer.py
```

You should see one row per month with five columns:
month, fire count, average temperature, total burned area, largest fire area.

### 3c — Step 7: Upload dataset to HDFS

```bash
hdfs dfs -mkdir -p /user/$USER/forest_fire/input

hdfs dfs -put -f forestfires.csv \
/user/$USER/forest_fire/input/

hdfs dfs -ls /user/$USER/forest_fire/input/
```

### 3c — Step 8: Find the Hadoop Streaming JAR

```bash
find "$HADOOP_HOME" -name "hadoop-streaming*.jar"
```

For the environment described in the uploaded guide, the path is:

```text
$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar
```

If your `find` command returns a different version/path, use the actual path returned by your computer.

### 3c — Step 9: Run the Hadoop Streaming job

```bash
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"

hdfs dfs -rm -r -f /user/$USER/forest_fire/output

hadoop jar "$STREAMING_JAR" \
-files mapper.py,reducer.py \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py" \
-input /user/$USER/forest_fire/input/forestfires.csv \
-output /user/$USER/forest_fire/output
```

### 3c — Step 10: Display MapReduce output

```bash
hdfs dfs -cat /user/$USER/forest_fire/output/part-*
```

Output columns:

```text
Month | Number of fires | Average temperature | Total burned area | Largest fire area
```

---

## 3c — PART B: APACHE HIVE

### 3c — Step 11: Start Hadoop services

If they are already running, do not start them again.

```bash
start-dfs.sh
start-yarn.sh
jps
```

### 3c — Step 12: Start HiveServer2

```bash
nohup hiveserver2 > ~/hiveserver2.log 2>&1 &
```

Wait for HiveServer2 to start before running Beeline commands.

### 3c — Step 13: Create the Hive database

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "CREATE DATABASE IF NOT EXISTS forest_fire_db"
```

### 3c — Step 14: Verify the database

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "SHOW DATABASES"
```

### 3c — Step 15: Create the external Hive table

The table points to the HDFS input directory uploaded in Part A.

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
CREATE EXTERNAL TABLE IF NOT EXISTS forest_fire_db.forestfires (
    X INT,
    Y INT,
    month STRING,
    day STRING,
    FFMC DOUBLE,
    DMC DOUBLE,
    DC DOUBLE,
    ISI DOUBLE,
    temp DOUBLE,
    RH INT,
    wind DOUBLE,
    rain DOUBLE,
    area DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/$USER/forest_fire/input'
TBLPROPERTIES ('skip.header.line.count'='1')
"
```

### 3c — Step 16: Check total records

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT COUNT(*)
FROM forest_fire_db.forestfires
WHERE month <> 'month'
"
```

### 3c — Step 17: Count fires per month

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT month, COUNT(*) AS fire_count
FROM forest_fire_db.forestfires
WHERE month <> 'month'
GROUP BY month
ORDER BY fire_count DESC
"
```

### 3c — Step 18: Average temperature by month

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT month, ROUND(AVG(temp), 2) AS avg_temperature
FROM forest_fire_db.forestfires
WHERE month <> 'month'
GROUP BY month
ORDER BY avg_temperature DESC
"
```

### 3c — Step 19: Total burned area by month

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT month, ROUND(SUM(area), 2) AS total_burned_area
FROM forest_fire_db.forestfires
WHERE month <> 'month'
GROUP BY month
ORDER BY total_burned_area DESC
"
```

### 3c — Step 20: Average humidity and wind speed by month

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT
    month,
    ROUND(AVG(RH), 2) AS avg_humidity,
    ROUND(AVG(wind), 2) AS avg_wind
FROM forest_fire_db.forestfires
WHERE month <> 'month'
GROUP BY month
ORDER BY month
"
```

### 3c — Step 21: Find the largest forest fire

```bash
beeline -u jdbc:hive2://localhost:10000/default \
-n "$USER" \
-e "
SELECT month, day, temp, RH, wind, area
FROM forest_fire_db.forestfires
WHERE month <> 'month'
ORDER BY area DESC
LIMIT 1
"
```

### 3c — Step 22: Stop Hadoop after finishing

```bash
stop-yarn.sh
stop-dfs.sh
```

---

# PRACTICAL 4c — SEVEN DATA VISUALIZATIONS USING TABLEAU

## Objective

Create seven types of data visualizations using Tableau and prepare a dashboard.

## 4c — Step 1: Open Tableau

Use Tableau Desktop or Tableau Public.

Tableau Public download:
https://www.tableau.com/products/public

## 4c — Step 2: Download datasets

Adult dataset:
https://archive.ics.uci.edu/dataset/2/adult

Iris dataset:
https://archive.ics.uci.edu/dataset/53/iris

Use the Iris dataset for the first three visualizations and the Adult dataset for the multidimensional and hierarchical visualizations.

## 4c — Step 3: Connect the Iris dataset

1. Open Tableau.
2. Under Connect, select Text File.
3. Select the Iris CSV file.
4. Open Sheet 1.

The Iris fields should be:
- SepalLength
- SepalWidth
- PetalLength
- PetalWidth
- Species

If the downloaded file uses different field names, use the corresponding columns.

## VISUALIZATION 1 — 1D HISTOGRAM

1. Right-click PetalLength in the Data pane.
2. Select Create → Bins.
3. Drag PetalLength (bin) to Columns.
4. Drag Number of Records to Rows.
5. Select Histogram from Show Me if needed.

## VISUALIZATION 2 — 2D SCATTER PLOT

1. Drag SepalLength to Columns.
2. Drag PetalLength to Rows.
3. Drag Species to Color.
4. Select Circle marks.

## VISUALIZATION 3 — 3D-STYLE VISUALIZATION

1. Drag SepalLength to Columns.
2. Drag SepalWidth to Rows.
3. Drag PetalLength to Size.
4. Drag Species to Color.

This is a three-variable visual encoding, not a true 3D spatial chart.

## VISUALIZATION 4 — TEMPORAL VISUALIZATION

The Adult and Iris datasets do not contain a suitable date field. Create a separate sample file called `temporal.csv` with the following data:

```csv
Date,Value
2026-01-01,10
2026-02-01,15
2026-03-01,20
2026-04-01,18
2026-05-01,25
2026-06-01,30
```

Then:

1. Connect `temporal.csv` using Text File.
2. Drag Date to Columns.
3. Drag Value to Rows.
4. Select Line in the Marks card.

This is simulated sample data, not a temporal analysis of the Adult or Iris datasets.

## VISUALIZATION 5 — MULTIDIMENSIONAL VISUALIZATION

Connect the Adult dataset.

If the Adult CSV does not have headers, assign these field names in this order:

Age, Workclass, Fnlwgt, Education, EducationNum, MaritalStatus, Occupation, Relationship, Race, Sex, CapitalGain, CapitalLoss, HoursPerWeek, NativeCountry, Income

Then:

1. Drag Age to Columns.
2. Drag HoursPerWeek to Rows.
3. Drag Income to Color.
4. Drag CapitalGain to Size.
5. Drag Occupation to Detail.

## VISUALIZATION 6 — TREE / HIERARCHICAL VISUALIZATION

Use the Adult dataset.

1. Drag Education to the Marks card.
2. Drag Occupation to Detail.
3. Drag Number of Records to Size.
4. Drag Education to Label.
5. Drag Occupation to Color.
6. Open Show Me.
7. Select Treemap.

## VISUALIZATION 7 — NETWORK VISUALIZATION

A true network graph needs a dataset with Source and Target columns (and optionally Weight), plus a compatible Tableau network graph extension.

If your practical environment does not have a network extension, use this relationship heatmap as an alternative visualization:

1. Drag Education to Columns.
2. Drag Occupation to Rows.
3. Drag Number of Records to Color.
4. Select Square marks.

A relationship heatmap is not a true node-edge network graph. If your examiner specifically requires a network graph, use a compatible network extension and an edge-list dataset.

## 4c — Step 4: Create a dashboard

1. Click New Dashboard.
2. Drag the created worksheets onto the dashboard.
3. Add the title: `Data Visualization using Tableau`.
4. Arrange the visualizations clearly.
5. Save the workbook.

## 4c — Step 5: Save the project

Save as a Tableau Packaged Workbook (`.twbx`) when available.

Expected result: the seven visualizations and a dashboard.

---

# GITHUB — UPLOAD THIS PROJECT AND CLONE IT AGAIN

## Step 1: Create a GitHub repository

1. Open https://github.com
2. Sign in.
3. Create a new repository named `BDA-Practicals`.
4. Create it as an empty repository (do not add another README if you already have this file).

## Step 2: Open VS Code

1. Create a folder named `BDA-Practicals`.
2. Save this entire document inside it as `README.md`.
3. Open the VS Code terminal in that folder.

## Step 3: Upload to GitHub

Replace the example URL with your own repository URL.

```bash
git --version

git config --global user.name "Your Name"
git config --global user.email "your-email@example.com"

git init
git add README.md
git commit -m "Add complete BDA practical guide"
git branch -M main

git remote add origin https://github.com/YOUR-USERNAME/BDA-Practicals.git
git push -u origin main
```

## Step 4: Clone the repository in Ubuntu

Replace the URL with your actual repository URL.

```bash
cd ~

git clone https://github.com/YOUR-USERNAME/BDA-Practicals.git

cd BDA-Practicals

cat README.md
```

## Step 5: Follow a practical from the cloned README

```bash
# Read the guide in the terminal
less README.md

# Press q to exit less
```

You can also open the cloned folder in VS Code:

```bash
code .
```

## Step 6: Update GitHub after editing

```bash
git status
git add README.md
git commit -m "Update BDA practical guide"
git push origin main
```

---

# FINAL CHECKLIST

- [ ] Practical 2c: Hadoop services start successfully.
- [ ] Practical 2c: Java source compiles and JAR is created.
- [ ] Practical 2c: Both MapReduce jobs complete successfully.
- [ ] Practical 2c: Per-user duration and maximum user are displayed.
- [ ] Practical 3c: Forest Fires CSV downloads successfully.
- [ ] Practical 3c: Local Python mapper/reducer test works.
- [ ] Practical 3c: Hadoop Streaming job completes.
- [ ] Practical 3c: Hive database and external table are created.
- [ ] Practical 3c: Hive queries return results.
- [ ] Practical 4c: Seven visualizations are created.
- [ ] Practical 4c: Tableau dashboard is saved.
- [ ] GitHub: README.md is pushed successfully.
- [ ] Ubuntu: Repository is cloned successfully.

# TROUBLESHOOTING

## Hadoop command not found

Your Ubuntu environment may not have Hadoop configured. Use the college-configured environment. Do not install random versions during the practical.

## Output directory already exists

Remove the output folder before rerunning the relevant job:

```bash
hdfs dfs -rm -r -f /user/$USER/login_project/output1
hdfs dfs -rm -r -f /user/$USER/login_project/output2
hdfs dfs -rm -r -f /user/$USER/forest_fire/output
```

## Hadoop Streaming JAR not found

Search for the actual JAR:

```bash
find "$HADOOP_HOME" -name "hadoop-streaming*.jar"
```

Use the exact path returned by the command in the `hadoop jar` command.

## Hive connection refused

Check whether HiveServer2 is running:

```bash
ps -ef | grep -i hiveserver2
tail -50 ~/hiveserver2.log
```

Start HiveServer2 again only if it is not already running:

```bash
nohup hiveserver2 > ~/hiveserver2.log 2>&1 &
```

## Git asks for authentication

Follow GitHub's current sign-in/token prompt. Do not put your password or personal access token inside this README or commit it to GitHub.
