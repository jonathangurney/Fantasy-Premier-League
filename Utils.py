# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 11:10:32 2024

@author: jonat
"""

import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

def gof_plots(response, residuals, fitted_values):
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(16, 4))
    
    plt.rcParams['lines.markersize'] = 5

    # QQ plot for residuals
    n_obs = len(residuals)
    quantiles = np.linspace(0, 1, n_obs+2)
    theoretical_quantiles = stats.norm.ppf(quantiles)
    ax1.scatter(theoretical_quantiles[1:-1], np.sort(residuals))
    a = np.linspace(-3, 3, 2)
    ax1.plot(a, a, color='black', linestyle='--')
    ax1.set_xlabel("Theoretical quantiles")
    ax1.set_ylabel("Sample quantiles")
    ax1.set_title("QQ plot of standardised residuals")

    # Plot of standardized residuals vs fitted values
    ax2.scatter(fitted_values, residuals, edgecolors='black', linewidths=0.2)
    ax2.set_xlabel("Fitted values")
    ax2.set_ylabel("Standardised residuals")
    ax2.set_title("Standardised residuals vs fitted values")

    # Plot of fitted values vs response
    ax3.scatter(response, fitted_values, edgecolor='black', linewidths=0.2)
    ax3.set_xlabel("Response")
    ax3.set_ylabel("Fitted values")
    ax3.set_title("Fitted values vs response")
    plt.show()
    
def split(a, n, shuffle=False):
    k = len(a) // n
    q = len(a) % n
    
    if shuffle:
        a = np.random.permutation(a)
    
    return [a[(i*k + min(i, q)):(i+1)*k + 1 + min(i, q-1)] for i in range(n)]