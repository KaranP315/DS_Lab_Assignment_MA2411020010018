import pandas as pd
import numpy as np

def optimize_dataframe():
    print("Generating DataFrame with 800,000 rows and 10 columns...")
    n_rows = 800000
    
    # 10 columns: mix of integers, floats, strings, and dates
    data = {
        'int_col1': np.random.randint(0, 100, size=n_rows),
        'int_col2': np.random.randint(-10000, 10000, size=n_rows),
        'float_col1': np.random.rand(n_rows) * 100.0,
        'float_col2': np.random.rand(n_rows) * 1000.0,
        'str_col1': np.random.choice(['Category A', 'Category B', 'Category C', 'Category D'], size=n_rows),
        'str_col2': np.random.choice(['Low', 'Medium', 'High'], size=n_rows),
        'str_col3': np.random.choice(['Yes', 'No'], size=n_rows),
        'date_col1': pd.date_range('2020-01-01', periods=n_rows, freq='s'),
        'date_col2': pd.date_range('2021-01-01', periods=n_rows, freq='s'),
        'float_col3': np.random.randn(n_rows) * 50.0
    }
    
    df = pd.DataFrame(data)
    
    # Print memory usage before optimization
    initial_memory = df.memory_usage(deep=True).sum()
    print(f"Memory Usage Before Optimization: {initial_memory / 1024**2:.2f} MB")
    
    # Optimize numeric columns (downcast)
    for col in df.columns:
        col_type = df[col].dtype
        
        if 'int' in str(col_type):
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif 'float' in str(col_type):
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif 'object' in str(col_type):
            # Check if converting to category saves memory
            num_unique = len(df[col].unique())
            num_total = len(df[col])
            # If the number of unique values is less than 50% of the total values, convert to category
            if num_unique / num_total < 0.5:
                df[col] = df[col].astype('category')
                
    # Print memory usage after optimization
    final_memory = df.memory_usage(deep=True).sum()
    print(f"Memory Usage After Optimization: {final_memory / 1024**2:.2f} MB")
    
    # Save optimized dataframe to Parquet format
    output_file = 'optimized_data.parquet'
    df.to_parquet(output_file)
    print(f"Optimized DataFrame saved to {output_file}")
    
    # Percentage memory saved
    memory_saved = initial_memory - final_memory
    percentage_saved = (memory_saved / initial_memory) * 100
    print(f"Total Memory Saved: {memory_saved / 1024**2:.2f} MB ({percentage_saved:.2f}%)")

if __name__ == "__main__":
    optimize_dataframe()
