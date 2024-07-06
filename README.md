# 🌍GlobalBlaze: Analyzing Global Trends in Wildfire Burned Areas and Emissions (2002-2023)

<img src="project\pictures\wildfire.png" width="800" height="466">

## Project Overview
This project investigates global wildfire trends and their impact on emissions from 2002 to 2023 using datasets from the Global Wildfire Information System (GWIS). It aims to analyze long-term trends, seasonal variations, regional prevalence, and the correlation between burned areas and emission gases to enhance understanding of wildfire patterns and their environmental implications.

### Datasets
1. [**Global Monthly Burned Area Dataset [2002-2023]**](https://gwis.jrc.ec.europa.eu/apps/country.profile/downloads): The dataset includes monthly burned area data (in hectares) from 2002 to 2023 for all countries and regions. It categorizes the data by types of land, such as forests, savannas, shrublands/grasslands, croplands, and others. This helps in analyzing fire patterns and their effects on different regions and types of land.
   
2. [**Global Monthly Emissions Dataset [2002-2023]**](https://gwis.jrc.ec.europa.eu/apps/country.profile/downloads): The dataset gives monthly data on how much pollution is released from burning plants (biomass) from 2002 to 2023. It shows how much CO2, CO, particles like dust and smoke (TPM and PM2.5), carbon, hydrocarbons, organic carbon, methane, sulfur dioxide, black carbon, and nitrogen oxides are released by different countries and regions. This helps to understand the environmental impact of burning plant materials.

## Tools and Technologies Used
- Data Analysis: Python (Pandas,Numpy)
- Visualization: Matplotlib, Seaborn
- Version Control: Git, GitHub

[**Project Data Report**](project/data-report.pdf): Document detailing data cleaning and pipeline procedures.

[**Project Analysis Report**](project/analysis-report.pdf): Final report containing data analysis and visualizations.

[**Project EDA**](project/EDA_report.ipynb): Notebook showcasing exploratory data analysis (EDA) for the project.

[**Presentation Slides**](project/slides.ppt)

[**Presenation Video Link**](project/presentation-video.md)

## Installation and Usage
Instructions for setting up the project environment and running the analysis scripts.

```bash
# Clone the repository
git clone https://github.com/puni-ram48/MADE-SS2024.git

# Install dependencies
pip install -r requirements.txt

```

## Data Pipeline and Testing

### Data Pipeline [here](project/automated_datapipeline.py)
Our project includes an automated data pipeline designed for wildfire analysis:

1. **Data Fetching**: Automatically retrieves monthly wildfire burned area and emission datasets from specified sources.
2. **Data Transformation and Cleaning**: Applies necessary transformations and cleans the data to ensure accuracy and consistency.
3. **Data Loading**: Transformed data is loaded into structured formats suitable for analysis, ensuring integrity for further investigation

This pipeline ensures that our wildfire data is prepared and maintained for reliable analysis of trends and impacts.

### Test Script [here](project/automated_testing.py)
We have developed a rigorous test script to validate our wildfire data pipeline:

1. Tests include verification of data fetching accuracy.
2. Ensures proper data cleaning and transformation procedures are followed.
3. Validates data integrity and consistency throughout the pipeline.

### Automated Workflow [here](.github/workflows/test_runner.yml)
To maintain the reliability of our wildfire data pipeline, we have set up an automated workflow using GitHub Actions:

* **Continuous Integration Tests**: Automatically runs our test script upon every push to the main branch.Ensures any updates or modifications do not compromise the functionality and accuracy of the data pipeline.
  
This automated workflow guarantees a robust and error-free approach to analyzing wildfire trends and impacts, ensuring high-quality project outcomes.

## How to Run the Data Pipeline and Tests
Provide detailed instructions on how to execute the data pipeline and run the test scripts. Include any necessary commands or steps to set up the environment.

```bash
# command to run the data pipeline
python3 automated_datapipeline.py

# command to execute the test script
python3 automated_testing.py
```

## Contributing
We welcome contributions to this project! If you would like to contribute, please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeature`).
3. Make your changes and commit them (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Create a new Pull Request.

Please ensure your code is well-documented.

## Authors and Acknowledgment
This project was initiated and completed by Puneetha Dharmapura Shrirama. 

## Special Thanks to Our Tutors:
I would like to extend my gratitude to our tutors **Philip Heltweg** and **Georg Schwarz** for their guidance and support throughout this project.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.
