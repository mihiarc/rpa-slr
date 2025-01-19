import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

# FIPS state code to state name mapping
STATE_NAMES = {
    '01': 'Alabama',
    '02': 'Alaska',
    '04': 'Arizona',
    '05': 'Arkansas',
    '06': 'California',
    '08': 'Colorado',
    '09': 'Connecticut',
    '10': 'Delaware',
    '11': 'District of Columbia',
    '12': 'Florida',
    '13': 'Georgia',
    '15': 'Hawaii',
    '16': 'Idaho',
    '17': 'Illinois',
    '18': 'Indiana',
    '19': 'Iowa',
    '20': 'Kansas',
    '21': 'Kentucky',
    '22': 'Louisiana',
    '23': 'Maine',
    '24': 'Maryland',
    '25': 'Massachusetts',
    '26': 'Michigan',
    '27': 'Minnesota',
    '28': 'Mississippi',
    '29': 'Missouri',
    '30': 'Montana',
    '31': 'Nebraska',
    '32': 'Nevada',
    '33': 'New Hampshire',
    '34': 'New Jersey',
    '35': 'New Mexico',
    '36': 'New York',
    '37': 'North Carolina',
    '38': 'North Dakota',
    '39': 'Ohio',
    '40': 'Oklahoma',
    '41': 'Oregon',
    '42': 'Pennsylvania',
    '44': 'Rhode Island',
    '45': 'South Carolina',
    '46': 'South Dakota',
    '47': 'Tennessee',
    '48': 'Texas',
    '49': 'Utah',
    '50': 'Vermont',
    '51': 'Virginia',
    '53': 'Washington',
    '54': 'West Virginia',
    '55': 'Wisconsin',
    '56': 'Wyoming'
}

def add_info_box(ax, counties):
    """Add an information box with key statistics to the map."""
    # Calculate statistics
    total_counties = len(counties)
    total_states = counties['STATEFP'].nunique()
    
    # Get top 3 states by county count
    top_states = counties.groupby('STATEFP').size().nlargest(3)
    
    # Create info box text with state names instead of codes
    title = 'COASTAL COUNTIES'
    info_text = [
        f'{total_counties:,}',
        'Total Counties',
        '',
        f'{total_states}',
        'Coastal States',
        '',
        'Top States:',
        f'{STATE_NAMES[top_states.index[0]]}  ({top_states.values[0]:,})',
        f'{STATE_NAMES[top_states.index[1]]}  ({top_states.values[1]:,})',
        f'{STATE_NAMES[top_states.index[2]]}  ({top_states.values[2]:,})'
    ]
    
    # Add semi-transparent background box with rounded corners
    box = Rectangle((0.02, 0.02), 0.28, 0.32, 
                   transform=ax.transAxes,
                   facecolor='white',
                   edgecolor='#666666',
                   alpha=0.85,
                   zorder=2,
                   linewidth=1,
                   clip_on=False)
    ax.add_patch(box)
    
    # Add title
    ax.text(0.03, 0.31, title,
            transform=ax.transAxes,
            fontsize=12,
            fontweight='bold',
            family='sans-serif',
            color='#2B8CBE',
            zorder=3)
    
    # Add statistics with different styles for numbers and labels
    y_pos = 0.27
    for i, text in enumerate(info_text):
        if i in [0, 3]:  # Large numbers
            ax.text(0.03, y_pos, text,
                   transform=ax.transAxes,
                   fontsize=16,
                   fontweight='bold',
                   family='sans-serif',
                   color='#08519C',
                   zorder=3)
            y_pos -= 0.025
        elif i in [1, 4]:  # Labels for numbers
            ax.text(0.03, y_pos, text,
                   transform=ax.transAxes,
                   fontsize=10,
                   family='sans-serif',
                   color='#666666',
                   zorder=3)
            y_pos -= 0.035
        elif i == 6:  # "Top States:" header
            ax.text(0.03, y_pos, text,
                   transform=ax.transAxes,
                   fontsize=10,
                   fontweight='bold',
                   family='sans-serif',
                   color='#2B8CBE',
                   zorder=3)
            y_pos -= 0.025
        elif i >= 7:  # State listings
            ax.text(0.03, y_pos, text,
                   transform=ax.transAxes,
                   fontsize=9,
                   family='sans-serif',
                   color='#333333',
                   zorder=3)
            y_pos -= 0.025
        else:  # Spacing
            y_pos -= 0.02

def create_coastal_map(coastal_counties_path: str, shoreline_path: str = "shoreline.parquet", output_path: str = "coastal_counties_map.png"):
    """Create a clean map visualization of coastal counties using Albers Equal Area projection.
    
    Args:
        coastal_counties_path: Path to the coastal counties parquet file
        shoreline_path: Path to the shoreline parquet file
        output_path: Path to save the map image
    """
    # Read the data
    counties = gpd.read_parquet(coastal_counties_path)
    shoreline = gpd.read_parquet(shoreline_path)
    
    # Set Albers Equal Area projection for continental US
    albers_proj = "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
    counties = counties.to_crs(albers_proj)
    shoreline = shoreline.to_crs(albers_proj)
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(15, 10))
    
    # Plot counties with a light fill
    counties.plot(
        ax=ax,
        color='#E6F3FF',  # Light blue fill
        edgecolor='#2B8CBE',  # Darker blue edges
        linewidth=0.8,  # Slightly thicker county lines
        alpha=0.9  # More opaque
    )
    
    # Plot shoreline with a clean, darker line
    shoreline.plot(
        ax=ax,
        color='#08519C',  # Even darker blue for shoreline
        linewidth=1.5,  # Thicker shoreline
        alpha=1.0  # Fully opaque
    )
    
    # Set equal aspect ratio for proper display
    ax.set_aspect('equal')
    
    # Customize the map
    ax.axis('off')  # Remove axes
    plt.title('US Coastal Counties and Shoreline', pad=20, fontsize=14)
    
    # Add information box
    add_info_box(ax, counties)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.2)
    print(f"Map saved to {output_path}")
    
def print_summary(coastal_counties_path: str):
    """Print a summary of the coastal counties.
    
    Args:
        coastal_counties_path: Path to the coastal counties parquet file
    """
    counties = gpd.read_parquet(coastal_counties_path)
    
    # Get summary statistics
    total_counties = len(counties)
    states = counties['STATEFP'].nunique()
    
    # Print summary
    print("\nCoastal Counties Summary")
    print("=" * 30)
    print(f"Total coastal counties: {total_counties}")
    print(f"Number of states: {states}")
    
    # Print counties by state with state names
    print("\nCounties by state:")
    state_counts = counties.groupby('STATEFP').size().sort_values(ascending=False)
    for state_code, count in state_counts.items():
        state_name = STATE_NAMES[state_code]
        print(f"{state_name}: {count} counties")

def main():
    """Create map and print summary of coastal counties."""
    coastal_counties_path = "coastal_counties.parquet"
    shoreline_path = "shoreline.parquet"
    
    # Create map
    create_coastal_map(coastal_counties_path, shoreline_path)
    
    # Print summary
    print_summary(coastal_counties_path)

if __name__ == "__main__":
    main() 