import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
import logging

# Bar graph   
def bar_graph(ax, categories, values, title="Bar Graph", xlabel="X-axis", ylabel="Y-axis", color='#EF233C', edgecolor='#2B2F42'):
    try:
        # Font and size setting
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)
        # Bar graph
        bars = ax.bar(categories, values, color=color, edgecolor=edgecolor)
        # Title and labels
        ax.set_title(title, fontsize=SIZE_LARGE,fontweight='bold')
        ax.set_xlabel(xlabel,fontsize = 15,fontweight='bold')
        ax.set_ylabel(ylabel,fontsize = 15,fontweight='bold')
        
        # Customizing the Axes Spines (Borders)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        # Only show ticks on the bottom spine
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        # remove ticks from x axis label as we are showing inside the bar
        ax.set_xticks([]) 
        
        # add category label inside the bar
        for bar, label in zip(bars, categories):
            ax.text(
                bar.get_x() + bar.get_width() / 2,  
                bar.get_y(),               
                f" {label}",                              
                color='white',                        
                ha='center',                         
                va='bottom',
                fontsize=SIZE_DEFAULT,               
                rotation=90
            )
        # add value at the top of the bar     
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,    
                bar.get_height(),                                   
                f'{height:.2f}',                     
                ha='center',                          
                va='bottom',                         
                fontsize=9,                         
                color='black'                        
            )
        # Grid for better readability
        ax.grid(axis='y', linestyle='--', alpha=0.7)     
    except Exception as e:
        logging.error(f"Error in plotting bar graph: {e}", exc_info=True)
        raise


# Horizontal bar graph    
def barh_chart(ax, categories, values, title="Bar Graph", xlabel="X-axis", ylabel="Y-axis", color='#EF233C', edgecolor='#2B2F42'):
    try:
        # Font and size settings
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)
        # Bar graph
        bars = ax.barh(categories, values, color=color, edgecolor=edgecolor)
        #start from top
        ax.invert_yaxis()
        
        # Title and labels
        ax.set_title(title, fontsize=SIZE_LARGE,fontweight='bold')
        ax.set_xlabel(xlabel,fontsize = 15, fontweight='bold')
        ax.set_ylabel(ylabel,fontsize = 15, fontweight='bold')
        
        # Customizing the Axes Spines (Borders)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        # Only show ticks on the bottom spine
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        
        # Add total values inside the bars
        for bar, value in zip(bars, values):
            ax.text(bar.get_width() - 0.01 * bar.get_width(),  # Position the text inside the bar
                    bar.get_y() + bar.get_height() / 2,
                    f'{value:.2f}', 
                    va='center', 
                    ha='right',  
                    fontsize=SIZE_DEFAULT, 
                    color='white',  
                    fontweight='bold'
                )
    except Exception as e:
        logging.error(f"Error in plotting horizontal bar graph: {e}", exc_info=True)
        raise

def plot_time_series(ax, categories, values, x_label="X-axis", y_label="Y-axis", title="Line Graph",
                    label=None, marker='o', linestyle='-', grid=True, color=None, legend_bg_alpha=0.5):
    try:
        # Font and size settings
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)
        
        # Title and labels
        ax.set_title(title, fontsize=SIZE_LARGE, fontweight='bold')
        ax.set_xlabel(x_label, fontsize=15, fontweight='bold')
        ax.set_ylabel(y_label, fontsize=15, fontweight='bold')
        
        # Customizing the Axes Spines (Borders)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        
        # Only show ticks on the bottom and left
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        
        # Default color if not provided
        if color is None:
            color = 'blue'  # Default color
        
        # Plotting a single line
        ax.plot(categories, values, marker=marker, linestyle=linestyle, color=color, label=label, lw=2) # lw:line width
        
        # Add data points and labels on the graph
        for x, y in zip(categories, values):
            ax.text(x, y, f'{y:.2f}', fontsize=9, ha='right', va='bottom', color=color)
        
        # Legend styling
        if label:
            legend = ax.legend(loc='best', fontsize=10, frameon=True, fancybox=True) #frameon=True: Enables a border or frame around the legend, fancybox=True: Adds rounded corners to the legend box
            legend.get_frame().set_alpha(legend_bg_alpha)  # Make legend transparent
        
        # Grid settings
        if grid:
            ax.grid(True, which='both', linestyle='--', linewidth=0.7, alpha=0.7)
        
        # Enhancing the plot
        plt.tight_layout()
    
    except Exception as e:
        logging.error(f"Error in plotting line graph: {e}", exc_info=True)
        raise


# stacked plot
def stacked_plot(ax,data, x_col, y_col, hue_col, title="title", x_label="x label", y_label="y label"):
    try:    
        # Font and size settings
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)       
        # Pivot the DataFrame to get a grouped bar chart format
        pivot_data = data.groupby([x_col, hue_col])[y_col].sum().unstack() 
        # Plot a stacked bar chart
        bars = pivot_data.plot(kind='bar', stacked=True, ax=ax, width=0.7)
            
        # Add values to each stack, excluding zeros
        for container in bars.containers:
            for rect in container:
                height = rect.get_height()
                if height > 0:  # Only label bars with a height greater than 0
                    ax.text(
                        rect.get_x() + rect.get_width() / 2,
                        rect.get_y() + height / 2,          
                        f'{height:.0f}',                     
                        ha='center', va='center',           
                        fontsize=10, color='white', weight='bold'
                    )
        # Customizing the Spines(Borders)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        # Only show ticks on the bottom spine
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        # Labeling
        ax.set_xlabel(x_label,fontsize = 15, fontweight='bold')
        ax.set_ylabel(y_label,fontsize = 15, fontweight='bold')
        ax.set_title(title, fontsize=SIZE_LARGE,fontweight='bold')
        ax.set_xticklabels(pivot_data.index, rotation=45, ha='right')
        
        # Add a legend
        ax.legend(title=hue_col, loc="upper left", fontsize=10, title_fontsize=12, frameon=True, framealpha=0.5)
        
        # Grid for better readability
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
    except Exception as e:
        logging.error(f"Error in plotting stacked bar graph: {e}", exc_info=True)
        raise    
        
# pie chart    
def plot_pie_chart(ax, sizes, labels, explode=None, autopct="%.2f%%", pctdistance=0.8, startangle=90, shadow=True, title="Pie Chart"):
    try:    
        # Font and size settings to match the histogram and line chart theme
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)
        # pie chart
        ax.pie(
            sizes,
            labels=labels,
            explode=explode,
            autopct=autopct,
            pctdistance=pctdistance,
            startangle=startangle,
            shadow=shadow
        )
        # Add a legend
        # ax.legend(title="Distribution", loc="best")
        
        # Set the title
        ax.set_title(title, fontsize=SIZE_LARGE,fontweight='bold')
        # Improve the layout
        plt.tight_layout()
    except Exception as e:
        logging.error(f"Error in creating pie Chart: {e}", exc_info=True)
        raise    


