# Advanced PySpark for Exploratory Data Analysis (EDA)

This project demonstrates advanced PySpark techniques for performing exploratory data analysis (EDA) on a large-scale fitness dataset. The dataset used is from the [FitRec Project](https://sites.google.com/eng.ucsd.edu/fitrec-project/home), which contains workout data including altitude, heart rate, speed, and timestamps. The goal of this project is to analyze and visualize the data to uncover insights about user activities, workout patterns, and gender-based trends.

---

## Dataset
The dataset is sourced from the [FitRec Project](https://sites.google.com/eng.ucsd.edu/fitrec-project/home). It contains workout data in JSON format, with attributes such as:
- `altitude`, `heart_rate`, `latitude`, `longitude`, `speed`, `timestamp` (arrays)
- `gender`, `sport`, `userId`, `id` (scalar values)

---

## Project Overview
The project is divided into the following steps:
1. **Initialization**: Setting up PySpark and loading the dataset into a PySpark DataFrame.
2. **Dataset Overview**: Exploring the schema, columns, and data types of the dataset.
3. **Exploratory Data Analysis (EDA)**:
   - Detecting missing values and abnormal data.
   - Summarizing user activity and workout statistics.
   - Analyzing the most popular sports and their participation by gender.
   - Exploring workout duration and start times.
   - Visualizing the distribution of records and intervals.
4. **Advanced Analysis**:
   - Creating new features from timestamps (e.g., workout duration, start time, intervals).
   - Converting DataFrame rows to RDDs for distributed processing.
   - Calculating statistics for intervals and heart rates.
5. **Visualization**: Generating plots to visualize trends and distributions.

---

## Key Features
- **Data Cleaning**: Handling missing values, abnormal zeros, and duplicate records.
- **Feature Engineering**: Creating new features like `workout_start_time`, `duration`, and `interval` from raw timestamps.
- **Statistical Analysis**: Calculating min, max, mean, percentiles, and standard deviation for key metrics.
- **Visualization**: Using Matplotlib to create histograms, bar charts, and pie charts for data exploration.

---

## Requirements
To run this project, you need the following:
- Python 3.7+
- PySpark (`pip install pyspark`)
- Pandas (`pip install pandas`)
- NumPy (`pip install numpy`)
- Matplotlib (`pip install matplotlib`)

---

## Installation
1. Clone the repository:
   ```python
   git clone https://github.com/your-username/advanced-pyspark-eda.git
   cd advanced-pyspark-eda
   ```

2. Install the required dependencies:
   ```python
   pip install -r requirements.txt
   ```

3. Download the dataset from the FitRec Project and place it in the input/fitrec-dataset/ directory.

## Usage
1. Open the Jupyter Notebook:
   ```python
   jupyter notebook
   ```
2. Run the notebook cells to perform EDA and generate visualizations.

## Results
The analysis reveals:
- The most popular sports are running, biking, and walking.
- Workout durations typically range from 30 minutes to 2 hours.
- Most workouts start in the morning or evening.
- Gender-based participation shows that males dominate most sports, but females have significant participation in activities like yoga and walking.

## Visualizations
1. Top 5 Sports by User Participation
- Bar and pie charts showing the most popular sports.

2. Workout Duration Distribution
- Histograms of workout durations by sport.

3. Workout Start Time Distribution
- Histograms of workout start times by sport and gender.

4. Interval Statistics
- Bar and line charts showing interval statistics (min, max, mean, percentiles).

5. Heart Rate Over Normalized Time
- Line plots of heart rate data over normalized time for each sport.

6. 3D Workout Paths
- 3D scatter and line plots showing workout paths in terms of longitude, latitude, and altitude.

## Advanced Analysis and Visualization
### Sampling Data for Analysis
To manage the large dataset, we sample a subset of users and workouts for detailed analysis. The sampling criteria are:

- Maximum users per gender: 20

- Maximum workouts per sport: 15

This ensures a balanced representation of users and activities while keeping the dataset manageable for visualization.

```python
# Use 2 variables to determine the sampling criteria:
# maximum users per gender and maximum workouts per sport
max_users_per_gender, max_workouts_per_sport = 20, 15

# Collect the sampled dataset to Pandas for visualization
pd_df = sampling_data(max_users_per_gender, max_workouts_per_sport).toPandas()
print('\nSampled data overview (only string and numeric columns):')
pd_df.describe()
```

### Normalized Time Analysis
To compare workouts of varying durations, we normalize the time for each workout by calculating the duration (in seconds) of each timestamp record relative to the first record of the workout. This allows us to plot heart rate data on a standardized timeline.

```python
# Normalize timestamps for each workout
normalized_datetime_list = []
for index, data_row in pd_df.iterrows():
    min_date_time = min(data_row['date_time'])
    normalized_datetime_list.append(
        [(date_time - min_date_time).seconds for date_time in data_row['date_time']]
    )

pd_df['normalized_date_time'] = normalized_datetime_list

# Plot heart rate over normalized time
sport_list = pd_df['sport'].unique()
fig, axs = plt.subplots(len(sport_list), figsize=(15, 6 * len(sport_list)))
for sport_index, sport in enumerate(sport_list):
    workout = pd_df[pd_df.sport == sport]
    for workout_index, data_row in workout.iterrows():
        label = 'user: ' + str(data_row['userId']) + ' - gender: ' + data_row['gender']
        axs[sport_index].plot(data_row['normalized_date_time'], data_row['heart_rate'], label=label)
    axs[sport_index].set_title('Activity: ' + sport, fontsize='small')
    axs[sport_index].set_xlabel('Time (sec)', fontsize='small')
    axs[sport_index].legend(loc='center left', bbox_to_anchor=(1.0, 0.5), prop={'size': 9})
plt.show()
```

### 3D Visualization of Workout Paths
We use 3D plots to visualize the displacement of workouts in terms of longitude, latitude, and altitude. This provides insights into the geographical and elevation changes during workouts.

```python
# Sample a smaller dataset for 3D visualization
pd_df_small = sampling_data(max_users_per_gender=2, max_workouts_per_sport=2).toPandas()

# Plot workout paths in 3D
workout_count = pd_df_small.shape[0]
ncols = 3
nrows = math.ceil(workout_count / ncols)
fig = plt.figure(figsize=(8 * (ncols + 0.5), 8 * nrows))
for row_index, row in pd_df_small.iterrows():
    ax = fig.add_subplot(nrows, ncols, row_index + 1, projection='3d')
    ax.scatter(row['longitude'], row['latitude'], row['altitude'], c='r', marker='o')
    ax.plot3D(row['longitude'], row['latitude'], row['altitude'], c='gray', label='Workout path')
    ax.set_title(f"Activity: {row['sport']} - Gender: {row['gender']}", fontsize=16)
    ax.set_xlabel('Longitude (°)', fontsize=16)
    ax.set_ylabel('Latitude (°)', fontsize=16)
    ax.set_zlabel('Altitude (m)', fontsize=16)
    ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
plt.tight_layout()
plt.show()
```

### Grouped Analysis by Sport
To analyze trends across different sports, we group the data by sport and visualize workout paths for a subset of users. This helps identify patterns in workout routes and elevation changes for specific activities.

```python
# Group data by sport and plot workout paths
grouped = smalldf.groupby('sport')
ncols = 3
nrows = math.ceil(len(grouped) / ncols)
fig = plt.figure(figsize=(8 * ncols, 6 * nrows))
for idx, (sport, group) in enumerate(grouped):
    ax = fig.add_subplot(nrows, ncols, idx + 1, projection='3d')
    for i, (_, row) in enumerate(group.iterrows()):
        ax.plot3D(row['longitude'], row['latitude'], row['altitude'], label=f"User {i + 1}")
        ax.scatter(row['longitude'], row['latitude'], row['altitude'], s=10, label=None)
    ax.set_title(f"{sport} - {len(group)} Users", fontsize=14)
    ax.set_xlabel("Longitude (°)", fontsize=12)
    ax.set_ylabel("Latitude (°)", fontsize=12)
    ax.set_zlabel("Altitude (m)", fontsize=12)
    if len(group) <= 10:
        ax.legend(loc='upper right', fontsize=8)
plt.tight_layout()
plt.show()
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
