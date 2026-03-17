## Project Background
TOYS.INC is a Mexican company founded in 1991 that sells a variety of toy products via their physical stores. 

The company has significant amounts of data on its sales from January 1st 2022 to September 30th 2023 on its profit margins and store locations that have been underutilised. This project analyses and synthesises this data to uncover insights to improve TOYS.INC's commercial success.

Insights and recommendations are provided on the following areas:
- **Profit Margin Analysis**: Evaluation of historical profit margins for different product categories 
- **Location comparisons**: An analysis of revenue and product level performance across store locations.

An interactive PowerBI dashboard can be downloaded [here](https://github.com/dtan20441/portfolio_toys/blob/main/maven_toys.pbix).

The SQL queries utilised to inspect and perform quality checks on the datasets can be found [here](https://github.com/dtan20441/portfolio_toys/blob/main/maven_toys.sql).

## Data Structure & Initial Checks
TOYS.INC's database structure consists of 4 tables: inventory, stores, products and sales, with a total row count of 830,940 records. 
![Entity relationship diagram](https://github.com/dtan20441/portfolio_toys/blob/main/Screenshots/Entity%20relationship%20diagram.png)

## Insights
### Overview of findings
From January 2022 to September 2023, profit margins varied by product category, with Electronics showing a downward trend, Games, Sports & Outdoors and Toys maintaining stable margins, and Arts & Crafts improving their margin.

Products earn the majority of their revenue from stores located in downtown. Hence, revenue from downtown stores are considerably higher than commercial, residential and airport based stores.

### Profit Margin Analysis
- Profit margins for all product categories are fairly high, with all categories having at least a 20% profit margin for majority for the time period.
- **Profit margin for Electronics was 48% at January 2022 and fell to 40% in September 2023.** This corresponds with a decrease in Electronics revenue in that same time period from about $143K to $73K.
- Revenue for Toys reached one of its lowest in July 2022 with $146k but experienced its largest profit margin of 30% in that time — suggesting cost efficiencies or a focus on high-margin products. Since then, revenue has rebounded while profit margin has plateaued at about 20%.
  
![Profit margin graph](https://github.com/dtan20441/portfolio_toys/blob/main/Screenshots/Electronics%20profit%20margin%20decline.png)

### Location comparisons
- Downtown stores generated $8.2 million in revenue, whereas all other store locations generated a combined $6.3 million. This can be attributed to the presence of more stores in downtown areas than others. 
- For every product, downtown stores contribute the majority share of revenue, followed by commercial locations. Residential and airport stores consistently trail behind. 
  
![Store revenue graph](https://github.com/dtan20441/portfolio_toys/blob/main/Screenshots/Store%20revenue.png)

## Recommendations
Based on the uncovered insights:
- With Electronics' profit margin dropping 8% and its revenue coming from 3 products, **diversifying the product range is crucial**. Expanding the Electronics category with new product lines would provide upsell opportunities and provide competition for competitors with a more extensive range. 
- Analyse what operational or pricing strategies were used in July 2022 for Toys that saw their highest profit margin despite low revenue and **assess whether they can be scaled or repeated.**
- For Toys, the revenue rebound has not led to improved margins, implying growth may be driven by lower-margin products. **Marketing should be shifted to higher-margin products** to limit or prevent decreasing margins. 
- **Re-evaluate Airport and Residential store locations.** As these stores contribute the least revenue and show low % of sales for all products, a profitability audit should be conducted to assess if the cost of running the stores long-term are justified or if its worth closing down and using the money to reinvest elsewhere. 
- Downtown stores consistently generate the highest revenue, with all listed products earning the majority of their sales from these locations. As a result, they should be **prioritized for expansion, increased staffing, and optimized inventory management** to further capitalize on their strong performance.

