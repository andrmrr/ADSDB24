import pandas
import os


if __name__ == '__main__':

    years = [2017, 2021]
    topics = []
    locations_to_exclude = ["United States", "Guam", "Puerto Rico", "Virgin Islands", 
            "United States, DC & Territories", "West", "Northeast", "South", "Midwest", "Florida"]
    # topics_to_exclude = ["Older Adults"
    output_dir = "sampled_datasets"
    datasets = os.listdir("../original_datasets")
    print(datasets)

    for dataset in datasets:
        for year in years:
            # Load the data
            df = pandas.read_csv(os.path.join("../original_datasets", dataset))
            df = df[df["YearStart"].isin(years)]
            df = df[~df["LocationDesc"].isin(locations_to_exclude)]
            print(df.shape)
            questions = df["Question"].value_counts()
            question_list = questions[questions == questions.max()].index.tolist()
            # print(question_list)
            df = df[df["Question"].isin(question_list)]
            print(df.shape)

            # Save the sampled data
            output_name = dataset.replace(".csv", f"_{year}.csv")
            df.to_csv(os.path.join(output_dir, output_name), index=False)