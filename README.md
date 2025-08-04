# portfolio_toys

## Project Background
TOYS.INC is a Mexican company founded in 1991 that sells a variety of toy products via their physical stores. 

The company has significant amounts of data on its sales from January 1st 2022 to September 30th 2023 on its profit margins and store locations that have been underutilised. This project analyses and synthesises this data to uncover insights to improve TOYS.INC's commercial success.

Insights and recommendations are provided on the following areas:
- **Profit Margin Analysis**: Evaluation of historical profit margins for different product categories 
- **Location comparisons**: An evaluation of revenue and product level revenue distribution across store locations.

An interactive PowerBI dashboard can be downloaded [here](https://github.com/dtan20441/portfolio_toys/blob/main/maven_toys.pbix).

The SQL queries utilised to inspect and perform quality checks on the datasets can be found [here](https://github.com/dtan20441/portfolio_toys/blob/main/maven_toys.sql).

## Data Structure & Initial Checks
TOYS.INC's database structure consists of 4 tables: inventory, stores, products and sales, with a total row count of 830,940 records. 
![Entity relationship diagram](https://github.com/dtan20441/portfolio_toys/blob/main/Entity%20relationship%20diagram.png)

## Executive Summary
### Overview of findings
From January 2022 to September 2023, profit margins varied by product category, with Electronics showing a downward trend, Games, Sports & Outdoors and Toys maintaining stable margins, and Arts & Crafts improving their margin.

Products earn the majority of their revenue from stores located in downtown. Hence, revenue from downtown stores are considerably higher than commercial, residential and airport based stores.

### Profit Margin Analysis
Profit margin for Electronics was 48% at January 2022 and fell to 40% in September 2023.
