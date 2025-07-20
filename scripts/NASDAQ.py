import pandas as pd
import requests
import matplotlib.pyplot as plt
from io import StringIO
import sys
from datetime import datetime
import json
import os

# Dictionary describing each column in nasdaqtraded.txt
COLUMN_DESCRIPTIONS = {
    "Symbol": "The ticker symbol uniquely identifying the security (e.g., AAPL for Apple Inc.).",
    "Security Name": "The full name or description of the security (e.g., 'Apple Inc. - Common Stock' or 'Goldman Sachs Physical Gold ETF Shares').",
    "Listing Exchange": "The primary exchange where the security is listed: N (NASDAQ), Y (NYSE), A (NYSE American), etc.",
    "Market Category": "For NASDAQ-listed securities, the market tier: Q (Global Select Market), G (Global Market), S (Capital Market). Blank for non-NASDAQ securities.",
    "ETF": "Indicates if the security is an Exchange-Traded Fund: Y (Yes), N (No). ETFs track indices, commodities, or baskets of assets.",
    "Round Lot Size": "The standard trading unit for the security (e.g., 100 shares for most stocks), used for quoting and trading purposes.",
    "Test Issue": "Indicates if the security is a test issue (not meant for actual trading): Y (Yes), N (No).",
    "Financial Status": "For NASDAQ-listed securities, indicates compliance status: N (Normal), D (Deficient, e.g., below listing standards), Q (Bankrupt), etc. Blank for non-NASDAQ securities.",
    "CQS Symbol": "Consolidated Quotation System symbol, used for non-NASDAQ securities in consolidated tape systems.",
    "NASDAQ Symbol": "The NASDAQ-specific symbol, often matching the Symbol column for NASDAQ-listed securities.",
    "NextShares": "Indicates if the security is a NextShares fund (a type of exchange-traded managed fund): Y (Yes), N (No)."
}

# Function to download and load the nasdaqtraded.txt file
def load_nasdaq_data(url="https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), sep="|", low_memory=False)
        df = df[df["Symbol"] != "File Creation Time"]
        df["Security Name"] = df["Security Name"].fillna("")
        df["ETF"] = df["ETF"].fillna("N")
        df["Test Issue"] = df["Test Issue"].fillna("N")
        df["NextShares"] = df["NextShares"].fillna("N")
        df["Financial Status"] = df["Financial Status"].fillna("N")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"Error parsing file: {e}")
        sys.exit(1)

# Function to categorize securities
def categorize_security(row):
    security_name = row["Security Name"]
    if not isinstance(security_name, str):
        return "Other"
    if row["ETF"] == "Y":
        return "ETF"
    elif "Common Stock" in security_name:
        return "Common Stock"
    elif "Preferred" in security_name:
        return "Preferred Stock"
    elif "Warrant" in security_name:
        return "Warrant"
    elif "Unit" in security_name:
        return "Unit"
    else:
        return "Other"

# Function to perform all analyses
def analyze_nasdaq_data(df):
    results = {}
    
    results["total_securities"] = len(df)
    results["etf_count"] = len(df[df["ETF"] == "Y"])
    
    df["Security Type"] = df.apply(categorize_security, axis=1)
    results["security_types"] = df["Security Type"].value_counts()
    
    results["exchange_counts"] = df["Listing Exchange"].value_counts()
    
    nasdaq_securities = df[df["Listing Exchange"] == "N"]
    results["market_categories"] = nasdaq_securities["Market Category"].value_counts()
    
    results["distressed_securities"] = df[
        (df["Listing Exchange"] == "N") & (df["Financial Status"] != "N")
    ][["Symbol", "Security Name", "Financial Status"]]
    
    results["lot_sizes"] = df["Round Lot Size"].value_counts()
    
    results["test_issues"] = len(df[df["Test Issue"] == "Y"])
    results["nextshares"] = len(df[df["NextShares"] == "Y"])
    
    return results, df

# Function to print column descriptions
def print_column_descriptions():
    print("\nColumn Descriptions for nasdaqtraded.txt:")
    for column, description in COLUMN_DESCRIPTIONS.items():
        print(f"- {column}: {description}")

# Function to print and save results
def print_and_save_results(results, df, output_file="analysis_output.txt"):
    # Capture output to both console and file
    with open(output_file, "w") as f:
        def write_output(text):
            print(text)
            f.write(text + "\n")
        
        write_output(f"Analysis of nasdaqtraded.txt (as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}):\n")
        
        write_output(f"1. Total Securities: {results['total_securities']}")
        write_output("   - Represents the total number of securities listed across all exchanges in the file.")
        
        write_output(f"\n2. Number of ETFs: {results['etf_count']}")
        write_output("   - Counts securities marked as ETFs (ETF = Y), which track indices, commodities, or asset baskets.")
        
        write_output("\n3. Security Type Breakdown:")
        write_output(results['security_types'].to_string())
        write_output("   - Categorizes securities based on ETF status and keywords in Security Name (e.g., Common Stock, Preferred Stock).")
        
        write_output("\n4. Securities by Exchange:")
        write_output(results['exchange_counts'].to_string())
        write_output("   - Shows the distribution of securities across exchanges (e.g., N = NASDAQ, Y = NYSE).")
        
        write_output("\n5. NASDAQ Market Categories:")
        write_output(results['market_categories'].to_string())
        write_output("   - For NASDAQ-listed securities, indicates market tiers: Q (Global Select, large-cap), G (Global, mid-cap), S (Capital, small-cap).")
        
        write_output("\n6. NASDAQ Securities with Financial Issues:")
        if not results['distressed_securities'].empty:
            write_output(results['distressed_securities'].to_string())
            write_output("   - Lists NASDAQ securities with non-normal financial status (e.g., D = Deficient, Q = Bankrupt), indicating compliance issues.")
        else:
            write_output("No distressed securities found.")
        
        write_output("\n7. Round Lot Size Distribution (Top 5):")
        write_output(results['lot_sizes'].head().to_string())
        write_output("   - Shows common trading unit sizes (e.g., 100 shares), affecting liquidity and trading practices.")
        
        write_output(f"\n8. Number of Test Issues: {results['test_issues']}")
        write_output("   - Counts test securities used for system testing, not actual trading.")
        
        write_output(f"\n9. Number of NextShares Funds: {results['nextshares']}")
        write_output("   - Counts NextShares funds, a type of actively managed ETF with unique trading mechanics.")
    
    # Save security types to CSV
    results["security_types"].to_csv("security_types.csv")
    print(f"\nSaved security types to 'security_types.csv'")

# Function to visualize data (using matplotlib)
def visualize_data(results):
    try:
        plt.figure(figsize=(8, 6))
        results["security_types"].plot(
            kind="pie",
            autopct="%1.1f%%",
            colors=["#36A2EB", "#FF6384", "#FFCE56", "#4BC0C0", "#9966FF"],
            title="Distribution of Security Types"
        )
        plt.ylabel("")
        plt.savefig("security_types_pie.png")
        print("Saved security types pie chart as 'security_types_pie.png'")
        plt.close()

        plt.figure(figsize=(10, 6))
        results["exchange_counts"].plot(
            kind="bar",
            color="#36A2EB",
            title="Securities by Exchange"
        )
        plt.xlabel("Exchange")
        plt.ylabel("Number of Securities")
        plt.savefig("exchange_counts_bar.png")
        print("Saved exchange counts bar chart as 'exchange_counts_bar.png'")
        plt.close()
    except ImportError:
        print("\nMatplotlib not installed. Skipping visualizations. Install with: pip install matplotlib")

# Chart.js visualization for security types
def create_chartjs_security_types(results, output_file="analysis_output.txt"):
    security_types = results["security_types"]
    labels = security_types.index.tolist()
    data = security_types.values.tolist()
    
    chart_config = {
        "type": "pie",
        "data": {
            "labels": labels,
            "datasets": [{
                "data": data,
                "backgroundColor": ["#36A2EB", "#FF6384", "#FFCE56", "#4BC0C0", "#9966FF"]
            }]
        },
        "options": {
            "title": {
                "display": True,
                "text": "Distribution of Security Types"
            }
        }
    }
    
    with open(output_file, "a") as f:
        f.write("\nChart.js Pie Chart for Security Types:\n")
        f.write("```chartjs\n")
        f.write(json.dumps(chart_config, indent=2))
        f.write("\n```\n")
    print("\nAppended Chart.js pie chart to 'analysis_output.txt'")

# Main function
def main():
    print("Loading nasdaqtraded.txt...")
    df = load_nasdaq_data()
    
    print_column_descriptions()
    
    print("\nAnalyzing data...")
    results, df = analyze_nasdaq_data(df)
    
    print_and_save_results(results, df)
    
    visualize_data(results)
    create_chartjs_security_types(results)

if __name__ == "__main__":
    main()
