## Project Plan

# GlobalBlaze: Analyzing Global Trends in Wildfire Burned Areas and Emissions (2002-2023)

## Main Question

1. What is the trend in global wildfire burned areas and emissions from 2002 to 2023?
2. How do wildfire burned areas vary across different landcover types (e.g., forest, savannas, croplands) and geographical regions?
3. How have emission levels of key pollutants (e.g., CO2, PM2.5) changed over time, and are there any noticeable trends or patterns?
4. Is there a correlation between wildfire area burned and emissions of diffrent gases?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
The main aim of the "GlobalBlaze" project is to investigate how wildfires release emission gases globally. This includes analyzing datasets spanning from 2002 to 2023, which contain information on
wildfire burned areas and emissions. By examining these datasets, the project aims to understand the dynamics of emission gases released during wildfires and identify patterns, correlations, and regional variations in emission levels. Ultimately, the goal is to provide insights into the relationship between wildfire activity and emissions, contributing to a better understanding of the environmental impact of wildfires and informing strategies for mitigation and management efforts.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Global Monthly Burned Area Dataset [2002-2023]
* Metadata URL:https://gwis.jrc.ec.europa.eu/apps/country.profile/downloads
* Data URL:https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/MCD64A1_burned_area_full_dataset_2002-2023.zip
* Data Type: Zipped CSV

The dataset provides monthly burned area(ha) data from 2002 to 2023, categorized by landcover classes, for all countries and sub-country administrative units. It includes information on forest, savannas, shrublands/grasslands, croplands, and other burned areas, facilitating analysis of fire patterns and impacts across different regions and land types.


### Datasource2: Global Monthly Emissions Dataset [2002-2023]
* Metadata URL:https://gwis.jrc.ec.europa.eu/apps/country.profile/downloads 
* Data URL: https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/emission_gfed_full_2002_2023.zip
* Data Type:Zipped CSV

The dataset provides monthly biomass burning emissions(Tons) data from 2002 to 2023, categorized by pollutant and covering all countries and sub-country administrative units.  It facilitates analysis of the environmental impact of biomass burning activities, including emissions of CO2, CO, TPM, PM2.5, TPC, NMHC, OC, CH4, SO2, BC, and NOx.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Dataset Search and Acquisition  [i1]
2. Project Planning  [i2] 
3. Data Cleaning and Preprocessing  [i3] 
4. Automated Data Pipeline Creation  [i4] 
5. Testing and Validation  [i5]
6. Create Visualizations  [i6] 
7. Correlation Analysis  [i7] 
8. Reporting and Documentation  [i8] 
9. Create Presentation  [i9]
10. Record Presentation Video  [i10]

[i1]: https://github.com/puni-ram48/MADE-SS2024/issues/1
[i2]: https://github.com/puni-ram48/MADE-SS2024/issues/2
[i3]: https://github.com/puni-ram48/MADE-SS2024/issues/3
[i4]: https://github.com/puni-ram48/MADE-SS2024/issues/4
[i5]: https://github.com/puni-ram48/MADE-SS2024/issues/5
[i6]: https://github.com/puni-ram48/MADE-SS2024/issues/6
[i7]: https://github.com/puni-ram48/MADE-SS2024/issues/7
[i8]: https://github.com/puni-ram48/MADE-SS2024/issues/8
[i9]: https://github.com/puni-ram48/MADE-SS2024/issues/9
[i10]: https://github.com/puni-ram48/MADE-SS2024/issues/10
