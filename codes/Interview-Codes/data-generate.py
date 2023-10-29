#/* Part for generate dataset */
import random
import numpy as np
import os

# Define the number of integers and the output directory
num_integers = 1000
output_directory = "data/"

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Generate a uniform distribution file
uniform_data = np.arange(1, num_integers + 1)
np.random.shuffle(uniform_data)
np.savetxt(output_directory + "uniform.txt", uniform_data, fmt='%d', delimiter="\n")

# Generate a Gaussian distribution file
mu, sigma = random.randint(0,1000), random.randint(0,1000)
gaussian_data = np.random.normal(mu, sigma, num_integers)
gaussian_data = gaussian_data.astype(int)
np.savetxt(output_directory + "gaussian.txt", gaussian_data, fmt='%d', delimiter="\n")

# Generate a Bernoulli distribution file
p = 0.5
bernoulli_data = np.random.binomial(1, p, num_integers)
np.savetxt(output_directory + "bernoulli.txt", bernoulli_data, fmt='%d', delimiter="\n")
