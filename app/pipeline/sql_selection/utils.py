from typing import Union, List, Optional, Tuple
import numpy as np
import warnings


def geometric_median(vectors: Union[List[List[float]], np.ndarray], 
                    max_iter: int = 1000, 
                    tolerance: float = 1e-5,
                    initial_guess: Optional[np.ndarray] = None,
                    verbose: bool = False) -> Tuple[np.ndarray, dict]:
    """
    Calculate the geometric median of a set of vectors (Geometric Median)
    
    The geometric median is the point that minimizes the sum of the Euclidean distances to all input vectors:
    argmin_z Σ ||z - vectors[i]||_2
    
    Use Weiszfeld algorithm to iteratively solve
    
    Parameters:
    -----------
    vectors : List[List[float]] or np.ndarray
        Input vector set, shape: (n_vectors, n_dimensions)
    max_iter : int, default=1000
        Maximum number of iterations
    tolerance : float, default=1e-5
        Convergence tolerance
    initial_guess : np.ndarray, optional
        Initial guess, if None then use arithmetic mean
    verbose : bool, default=False
        Whether to print debug information
        
    Returns:
    --------
    Tuple[np.ndarray, dict]:
        - Geometric median vector
        - Dictionary containing convergence information
    """
    
    if isinstance(vectors, list):
        vectors = np.array(vectors, dtype=np.float64)
    else:
        vectors = np.asarray(vectors, dtype=np.float64)
    
    if vectors.ndim != 2:
        raise ValueError(f"Vectors must be a 2D array, current dimension: {vectors.ndim}")
    
    n_vectors, n_dims = vectors.shape
    
    if n_vectors == 0:
        raise ValueError("Vectors cannot be empty")
    
    if n_vectors == 1:
        return vectors[0].copy(), {"converged": True, "iterations": 0, "final_objective": 0.0}
    
    # initialize
    if initial_guess is None:
        # use arithmetic mean as initial guess
        median = np.mean(vectors, axis=0)
    else:
        median = np.asarray(initial_guess, dtype=np.float64)
        if median.shape != (n_dims,):
            raise ValueError(f"Initial guess dimension mismatch: expected {n_dims}, got {median.shape}")
    
    prev_median = median.copy()
    converged = False
    
    for iteration in range(max_iter):
        # calculate the distance of each vector to the current median
        distances = np.linalg.norm(vectors - median, axis=1)
        
        # handle the case where the distance is 0 (avoid division by zero)
        # if a vector coincides with the current median, give it a very small distance
        distances = np.maximum(distances, tolerance)
        
        # calculate the weights: w_i = 1 / ||x_i - median||
        weights = 1.0 / distances
        
        # update the median: median = Σ(w_i * x_i) / Σ(w_i)
        weighted_sum = np.sum(weights[:, np.newaxis] * vectors, axis=0)
        weight_sum = np.sum(weights)
        new_median = weighted_sum / weight_sum
        
        # check the convergence
        change = np.linalg.norm(new_median - median)
        
        if verbose and (iteration % 100 == 0 or iteration < 10):
            objective = np.sum(np.linalg.norm(vectors - new_median, axis=1))
            print(f"Iteration {iteration}: change = {change:.6f}, objective = {objective:.6f}")
        
        if change < tolerance:
            converged = True
            median = new_median
            break
        
        median = new_median
    
    # calculate the final objective value
    final_objective = np.sum(np.linalg.norm(vectors - median, axis=1))
    
    # convergence information
    info = {
        "converged": converged,
        "iterations": iteration + 1,
        "final_change": change if 'change' in locals() else 0.0,
        "final_objective": final_objective,
        "tolerance": tolerance,
        "max_iter": max_iter
    }
    
    if not converged:
        warnings.warn(f"Algorithm did not converge after {max_iter} iterations, final change: {change:.6f}")
    
    return median, info


if __name__ == "__main__":
    vectors = np.array([[1, 2], [3, 4], [5, 6]])
    median, info = geometric_median(vectors)
    print(median)
    print(info)