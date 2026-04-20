import pandas as pd
import numpy as np

def clean_retail_sales():
    df = pd.read_csv('retail_sales.csv')
    
    # Calculate medians
    regional_category_median = df.groupby(['ProductCategory', 'Region'])['SalesAmount'].transform('median')
    category_median = df.groupby('ProductCategory')['SalesAmount'].transform('median')
    
    # Create the Imputation_Method column
    df['Imputation_Method'] = np.nan
    df['Imputation_Method'] = df['Imputation_Method'].astype('object')
    
    # Condition 1: Missing SalesAmount, Category Electronics, Region South
    # But instruction says: Some SalesAmount values are missing only for "Electronics" category in "South" region.
    # Instruction: Fills missing SalesAmount using the median of the same ProductCategory and Region.
    # If the median cannot be calculated, use the overall median of that ProductCategory.
    
    missing_mask = df['SalesAmount'].isnull()
    
    # Try filling with Regional Median
    can_fill_regional = missing_mask & regional_category_median.notnull()
    df.loc[can_fill_regional, 'SalesAmount'] = regional_category_median[can_fill_regional]
    df.loc[can_fill_regional, 'Imputation_Method'] = 'Regional_Median'
    
    # Update missing mask
    missing_mask = df['SalesAmount'].isnull()
    
    # Try filling with Category Median
    can_fill_category = missing_mask & category_median.notnull()
    df.loc[can_fill_category, 'SalesAmount'] = category_median[can_fill_category]
    df.loc[can_fill_category, 'Imputation_Method'] = 'Category_Median'
    
    # Save the cleaned file
    df.to_csv('cleaned_retail_sales.csv', index=False)
    
    # Print the count of rows filled by each method
    counts = df['Imputation_Method'].value_counts()
    print("Missing values filled using each method:")
    print(counts)
    
if __name__ == "__main__":
    clean_retail_sales()
