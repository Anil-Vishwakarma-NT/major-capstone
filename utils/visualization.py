import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging

# Bar graph   
def bar_graph(ax, categories, values, title="Bar Graph", xlabel="X-axis", ylabel="Y-axis", color='#EF233C', edgecolor='#2B2F42'):
    try:
        # Font and size settings to match the histogram and line chart theme
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
        # Hide unnecessary spines for consistency with the line chart and histogram
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        # Only show ticks on the bottom spine
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        # ax.spines["bottom"].set_bounds(min(categories), max(categories))
        ax.set_xticks([])
        for bar, label in zip(bars, categories):
            ax.text(
                bar.get_x() + bar.get_width() / 2,  
                bar.get_y(),               
                f" {label}",                              
                color='#2B2F42',                        
                ha='center',                         
                va='bottom',
                fontsize=SIZE_DEFAULT,               
                rotation=90
            )
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
        
        # Hide unnecessary spines for consistency
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
                    ha='right',  # Align the text to the right
                    fontsize=SIZE_DEFAULT, 
                    color='white',  # Text color set to white for contrast
                    fontweight='bold'
                )
    except Exception as e:
        logging.error(f"Error in plotting horizontal bar graph: {e}", exc_info=True)
        raise

# plot line graph
def plot_time_series(ax, categories, values, x_label="X-axis", y_label="Y-axis", title="Line Graph",
                    labels=None, markers=None, linestyles=None, grid=True, colors=None, legend_bg_alpha=0.5):
    try:
        # Font and size settings
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)
        # Title and labels
        ax.set_title(title, fontsize=SIZE_LARGE,fontweight='bold')
        ax.set_xlabel(x_label,fontsize = 15, fontweight='bold')
        ax.set_ylabel(y_label,fontsize = 15, fontweight='bold')
        # Hide unnecessary spines for consistency
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["top"].set_visible(False)
        # Only show ticks on the bottom spine
        ax.xaxis.set_ticks_position("bottom")
        ax.yaxis.set_ticks_position("left")
        # Default color palette if not provided
        if colors is None:
            colors = plt.cm.get_cmap('tab10', len(values)).colors  # Using 'tab10' color map
        # Plotting lines
        for i, value in enumerate(values):
            marker = markers[i] if markers and i < len(markers) else 'o'
            linestyle = linestyles[i] if linestyles and i < len(linestyles) else '-'
            color = colors[i % len(colors)]  # Cycle through the colors
            ax.plot(categories, value, marker=marker, linestyle=linestyle, color=color, label=labels[i] if labels else None, lw=2)
            # Add data points and labels on the graph
            for x, y in zip(categories, value):
                ax.text(x, y, f'{y:.2f}', fontsize=9, ha='right', va='bottom', color=color)
        # Legend styling
        if labels:
            legend = ax.legend(loc='best', fontsize=10, frameon=True, fancybox=True)
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
def stacked_plot(ax,data, x_col, y_col, hue_col, title="title", x_label="x label", y_label="y label", figsize=(10, 6)):
    try:    
        # Font and size settings
        SIZE_DEFAULT = 13
        SIZE_LARGE = 20
        plt.rc("font", weight="normal")
        plt.rc("font", size=SIZE_DEFAULT)       
        # Pivot the DataFrame to get a grouped bar chart format
        pivot_data = data.groupby([x_col, hue_col])[y_col].sum().unstack() 
        # Plot a stacked bar chart
        pivot_data.plot(kind='bar', stacked=True, ax=ax, width=0.7)
        # Hide unnecessary spines for consistency with the line chart and histogram
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
        ax.legend(title="Interaction Type", loc="upper left",)
        
        # Grid for better readability
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
    except Exception as e:
        logging.error(f"Error in plotting stacked bar graph: {e}", exc_info=True)
        raise    
        
# pie chart    
def plot_pie_chart(ax, sizes, labels, explode=None, autopct="%.2f%%", pctdistance=0.8, startangle=90, shadow=True, title="Pie Chart", figsize=(6, 6)):
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


