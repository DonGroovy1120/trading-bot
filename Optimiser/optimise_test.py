from skopt import Optimizer
from skopt import forest_minimize
from skopt.callbacks import DeltaXStopper
from skopt.learning import ExtraTreesRegressor
from skopt.space import Categorical, Integer, Real
from skopt.utils import use_named_args
import numpy as np
import pandas as pd

def _func(dicto):
    print(dicto)
    return dicto[0]*2+dicto[1]
dimmension = []
dimmension.append(Real(low=0, high=100, name="KEY"))
dimmension.append(Real(low=0, high=100, name="KEY2"))
forest_minimize(_func, dimmension)


#                res = forest_minimize(
#                    func=objective_function,
#                    dimensions=dimensions,
#                    n_calls=max_tries,
#                    base_estimator=ExtraTreesRegressor(n_estimators=20, min_samples_leaf=2),
#                    acq_func='LCB',
#                    kappa=3,
#                    n_initial_points=min(max_tries, 20 + 3 * len(kwargs)),
#                    initial_point_generator='lhs',  # 'sobel' requires n_initial_points ~ 2**N
#                    callback=DeltaXStopper(9e-7),
#                    random_state=random_state)