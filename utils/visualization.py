import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# histogram bar

def bar_plot(dataframe, x_col, y_col, values_col=None, title="", x_label="", y_label="", legend_title=""):
    # Plotting the results
    plt.figure(figsize=(10, 6))
    bars = plt.bar(dataframe[x_col], dataframe[y_col], color='skyblue')

    # Adding Country Names on Top of Each Bar
    if values_col:
        for bar, value in zip(bars, dataframe[values_col]):
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval - (yval * 0.1), value, ha='center', va='top', fontsize=8, fontweight='bold', color='black',rotation=90)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(legend_title)
    plt.xticks(rotation=45, ha='right') 
    plt.show()
    
    
def bar_graph(ax, categories, values, title="Bar Graph", xlabel="X-axis", ylabel="Y-axis", color='#EF233C', edgecolor='#2B2F42'):
    # fig, ax = plt.subplots(figsize=(10, 6))
    # Font and size settings to match the histogram and line chart theme
    SIZE_DEFAULT = 13
    SIZE_LARGE = 20
    plt.rc("font", weight="normal")
    plt.rc("font", size=SIZE_DEFAULT)
    plt.rc("axes", titlesize=SIZE_DEFAULT)
    plt.rc("axes", labelsize=SIZE_DEFAULT)
    plt.rc("xtick", labelsize=SIZE_DEFAULT)
    plt.rc("ytick", labelsize=SIZE_DEFAULT)
    # Bar graph
    bars = ax.bar(categories, values, color=color, edgecolor=edgecolor)
    # Title and labels
    ax.set_title(title, fontsize=SIZE_LARGE)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
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
            bar.get_x() + bar.get_width() / 2,  # x-coordinate (center of the bar)
            20,               # y-coordinate (middle of the bar)
            label,                              # text label
            color='#2B2F42',                        # text color
            ha='center',                         # Horizontal alignment: center
            va='bottom',
            fontsize=SIZE_DEFAULT,               # font size
            rotation=90
        )
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,   # X position: center of the bar
            bar.get_height(),                                   # Y position: slightly above the x-axis
            f'{int(height)}',                    # Text: the height value of the bar
            ha='center',                         # Horizontal alignment: center
            va='bottom',                         # Vertical alignment: bottom
            fontsize=12,                         # Font size
            color='black'                        # Text color
        ) 
        
        

def histogram(ax, data, bins=20, title="Histogram", xlabel="X-axis", ylabel="Y-axis", color='#EF233C', edgecolor='#2B2F42'):
    # fig, ax = plt.subplots(figsize=(10, 6))
    # Font and size settings to match the line chart theme
    SIZE_DEFAULT = 10
    SIZE_LARGE = 20
    plt.rc("font", weight="normal")
    plt.rc("font", size=SIZE_DEFAULT)
    plt.rc("axes", titlesize=SIZE_LARGE)
    plt.rc("axes", labelsize=SIZE_LARGE)
    plt.rc("xtick", labelsize=SIZE_DEFAULT)
    plt.rc("ytick", labelsize=SIZE_DEFAULT)
    # Histogram
    ax.hist(data, bins=bins, color=color, edgecolor=edgecolor)
    # Title and labels
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    # Hide unnecessary spines for consistency with the line chart
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    # Only show ticks on the bottom spine
    ax.xaxis.set_ticks_position("bottom")
    ax.yaxis.set_ticks_position("left")
    ax.spines["bottom"].set_bounds(min(data), max(data))
    # Display the plot
    # plt.tight_layout()
    # plt.show()           