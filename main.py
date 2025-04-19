from deap import base, creator, gp
import numpy as np
from alpha.deap_patch import *  # noqa
# 创建一个新类 FitnessMax，参数weights:(1.0,)，就是最大化
from alpha import *
from datafeed.expr_functions import unary_funcs
from datafeed import ts_rolling_funcs
from alpha.init_pset import EXPR, dummy, _random_int_

creator.create("FitnessMax", base.Fitness, weights=(1.0,))
# creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMax)

from deap import base, creator, tools

creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMax)

toolbox = base.Toolbox()

# pset = get_pset()
pset = gp.PrimitiveSetTyped("MAIN", [], EXPR)

# pset = add_operators_base(pset)
"""基础算子"""
# 无法给一个算子定义多种类型，只好定义多个不同名算子，之后通过helper.py中的convert_inverse_prim修正
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fadd')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fsub')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fmul')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fdiv')

pset.addPrimitive(dummy, [EXPR, int], EXPR, name='iadd')
pset.addPrimitive(dummy, [EXPR, int], EXPR, name='isub')
pset.addPrimitive(dummy, [EXPR, int], EXPR, name='imul')
pset.addPrimitive(dummy, [EXPR, int], EXPR, name='idiv')

# add_unary_ops(pset) 用下面的代码代替
for func in unary_funcs:
  pset.addPrimitive(dummy, [EXPR], EXPR, name=func)

# add_unary_rolling_ops(pset) 用下面的代码代替
for func in ts_rolling_funcs:
  pset.addPrimitive(dummy, [EXPR, int], EXPR, name=func)

# add_period_ops(pset)
# add_binary_ops(pset)
# add_binary_rolling_ops(pset)


pset.addEphemeralConstant('_random_int_', _random_int_, int)

pset.addTerminal(1, EXPR, name='open')
pset.addTerminal(1, EXPR, name='high')
pset.addTerminal(1, EXPR, name='low')
pset.addTerminal(1, EXPR, name='close')
pset.addTerminal(1, EXPR, name='volume')

toolbox.register("expr", gp.genHalfAndHalf, pset=pset, min_=1, max_=5)
toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
# print(toolbox.individual())
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

toolbox.register("evaluate", print)  # 、，在map中一并做了

toolbox.register("select", tools.selTournament, tournsize=3)  # 目标优化
# toolbox.register("select", tools.selNSGA2)  # 多目标优化 FITNESS_WEIGHTS = (1.0, 1.0)
toolbox.register("mate", gp.cxOnePoint)
toolbox.register("expr_mut", gp.genFull, min_=0, max_=2)
toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr_mut, pset=pset)

toolbox.register('map', backtester)

import operator

toolbox.decorate("mate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))
toolbox.decorate("mutate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))

# 这里定义初始化的因子数，可以得行修改
print('开始生成因子...')
pop = toolbox.population(10)
for p in pop:
  print(stringify_for_sympy(p))

hof = tools.HallOfFame(10)
# 只统计一个指标更清晰
stats = tools.Statistics(lambda ind: ind.fitness.values)
# 打补丁后，名人堂可以用nan了，如果全nan会报警
stats.register("avg", np.nanmean, axis=0)
stats.register("std", np.nanstd, axis=0)
stats.register("min", np.nanmin, axis=0)
stats.register("max", np.nanmax, axis=0)

# 使用修改版的eaMuPlusLambda
population, logbook = eaMuPlusLambda(pop, toolbox,
                                     # 选多少个做为下一代，每次生成多少新个体
                                     mu=150, lambda_=100,
                                     # 交叉率、变异率，代数
                                     cxpb=0.5, mutpb=0.1, ngen=3,
                                     # 名人堂参数
                                     # alpha=0.05, beta=10, gamma=0.25, rho=0.9,
                                     stats=stats, halloffame=hof, verbose=True,
                                     # 早停
                                     early_stopping_rounds=5)

print(logbook)

print('=' * 60)


def print_population(population):
  for p in population:
    expr = stringify_for_sympy(p)
    print(expr, p.fitness)


print_population(hof)
