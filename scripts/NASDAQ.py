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

# Mapping of exchange codes to full names
EXCHANGE_NAMES = {
    "N": "NASDAQ",
    "Y": "NYSE",
    "A": "NYSE American",
    "P": "NYSE Arca",
    "Z": "BATS",
    "V": "IEX",
    "": "Other"
}

# Simple sector inference based on Security Name keywords
SECTOR_KEYWORDS = {
    "Technology": ["Technology", "Tech", "Software", "Semiconductor", "Internet"],
    "Financial": ["Bank", "Financial", "Insurance", "Capital"],
    "Healthcare": ["Health", "Pharma", "Biotech", "Medical"],
    "Energy": ["Energy", "Oil", "Gas"],
    "Consumer": ["Consumer", "Retail", "Goods"]
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
        df["Listing Exchange"] = df["Listing Exchange"].fillna("")
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

# Function to infer sector from Security Name
def infer_sector(row):
    security_name = row["Security Name"]
    if not isinstance(security_name, str):
        return "Unknown"
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(keyword in security_name for keyword in keywords):
            return sector
    return "Unknown"

# Function to perform all analyses
def analyze_nasdaq_data(df):
    results = {}
    
    results["total_securities"] = len(df)
    results["etf_count"] = len(df[df["ETF"] == "Y"])
    
    df["Security Type"] = df.apply(categorize_security, axis=1)
    results["security_types"] = df["Security Type"].value_counts()
    
    df["Exchange Name"] = df["Listing Exchange"].map(EXCHANGE_NAMES).fillna("Other")
    results["exchange_counts"] = df["Exchange Name"].value_counts()
    
    results["exchange_security_types"] = df.pivot_table(
        index="Exchange Name",
        columns="Security Type",
        values="Symbol",
        aggfunc="count",
        fill_value=0
    )
    
    nasdaq_securities = df[df["Listing Exchange"] == "N"]
    results["market_categories"] = nasdaq_securities["Market Category"].value_counts()
    
    results["distressed_securities"] = df[
        (df["Listing Exchange"] == "N") & (df["Financial Status"] != "N")
    ][["Symbol", "Security Name", "Financial Status"]]
    
    # Total number of warrant stocks
    results["warrant_count"] = len(df[df["Security Type"] == "Warrant"])
    
    # Infer sectors and get top 5
    df["Sector"] = df.apply(infer_sector, axis=1)
    results["sector_counts"] = df["Sector"].value_counts().head(5)
    
    # Top 5 securities by round lot size
    results["top_lot_sizes"] = df[["Symbol", "Security Name", "Round Lot Size"]].nlargest(5, "Round Lot Size")
    
    return results, df

# Function to print column descriptions
def print_column_descriptions():
    print("\nColumn Descriptions for nasdaqtraded.txt:")
    for column, description in COLUMN_DESCRIPTIONS.items():
        print(f"- {column}: {description}")
    print("\nExchange Code Mappings:")
    for code, name in EXCHANGE_NAMES.items():
        print(f"- {code}: {name}")

# Function to print and save results
def print_and_save_results(results, df, output_file="analysis_output.txt"):
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
        write_output("   - Shows the distribution of securities across exchanges (e.g., NASDAQ, NYSE).")
        
        write_output("\n5. Exchange vs. Security Type Breakdown:")
        write_output(results['exchange_security_types'].to_string())
        write_output("   - Shows the count of each security type per exchange (e.g., ETFs on NASDAQ).")
        
        write_output("\n6. NASDAQ Market Categories:")
        write_output(results['market_categories'].to_string())
        write_output("   - For NASDAQ-listed securities, indicates market tiers: Q (Global Select, large-cap), G (Global, mid-cap), S (Capital, small-cap).")
        
        write_output("\n7. NASDAQ Securities with Financial Issues:")
        if not results['distressed_securities'].empty:
            write_output(results['distressed_securities'].to_string())
            write_output("   - Lists NASDAQ securities with non-normal financial status (e.g., D = Deficient, Q = Bankrupt), indicating compliance issues.")
        else:
            write_output("No distressed securities found.")
        
        write_output(f"\n8. Number of Warrant Stocks: {results['warrant_count']}")
        write_output("   - Counts securities categorized as warrants (e.g., rights to buy stock at a specific price).")
        
        write_output("\n9. Top 5 Sectors:")
        write_output(results['sector_counts'].to_string())
        write_output("   - Inferred from Security Name keywords, showing the most common industries.")
        
        write_output("\n10. Top 5 Securities by Round Lot Size:")
        write_output(results['top_lot_sizes'].to_string())
        write_output("   - Lists securities with the largest round lot sizes, indicating higher trading units.")

# Function to visualize data
def visualize_data(results):
    try:
        # Create a figure with two subplots
        fig = plt.figure(figsize=(16, 6))
        
        # Pie chart for security types with improved labeling
        ax1 = fig.add_subplot(121)
        security_types = results["security_types"]
        labels = security_types.index
        sizes = security_types.values
        ax1.pie(
            sizes,
            labels=labels,
            autopct=lambda pct: f"{pct:.1f}%\n({int(pct * sum(sizes) / 100):d})",
            colors=["#36A2EB", "#FF6384", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"],
            startangle=90,
            textprops={"fontsize": 10}
        )
        ax1.set_title("Distribution of Security Types")
        ax1.legend(labels, loc="center left", bbox_to_anchor=(1, 0.5), title="Security Types")
        
        # Stacked bar chart for exchange vs. security types
        ax2 = fig.add_subplot(122)
        exchange_security_types = results["exchange_security_types"]
        exchange_security_types.plot(
            kind="bar",
            stacked=True,
            ax=ax2,
            color=["#36A2EB", "#FF6384", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"]
        )
        ax2.set_title("Securities by Exchange and Type")
        ax2.set_xlabel("Exchange")
        ax2.set_ylabel("Number of Securities")
        ax2.legend(title="Security Type")
        
        # Annotate the exchange with the most ETFs
        max_etf_exchange = exchange_security_types["ETF"].idxmax()
        max_etf_count = exchange_security_types["ETF"].max()
        ax2.annotate(
            f"Most ETFs: {max_etf_exchange} ({max_etf_count})",
            xy=(exchange_security_types.index.get_loc(max_etf_exchange), exchange_security_types.loc[max_etf_exchange].sum()),
            xytext=(0, 10),
            textcoords="offset points",
            ha="center",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="black", lw=1)
        )
        
        plt.tight_layout()
        plt.savefig("nasdaq_analysis.png")
        print("Saved combined analysis chart as 'nasdaq_analysis.png'")
        plt.close()
    except ImportError:
        print("\nMatplotlib not installed. Skipping visualizations. Install with: pip install matplotlib")

# Chart.js visualization for security types
def create_chartjs_security_types(results, output_file="analysis_output.txt"):
    security_types = results["security_types"]
    labels = [f"{idx}: {val}" for idx, val in zip(security_types.index, security_types.values)]
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
