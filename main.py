from alpha.deap_patch import *  # noqa
# 创建一个新类 FitnessMax，参数weights:(1.0,)，就是最大化
from alpha import *
from datafeed.expr_functions import unary_funcs
from datafeed import ts_rolling_funcs
from alpha.init_pset import EXPR, dummy, _random_int_
from deap import base, creator, tools
import operator
import pandas as pd
import os

creator.create("FitnessMax", base.Fitness, weights=(1.0,))

creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMax)

toolbox = base.Toolbox()
terminal = 'open'
# pset = get_pset() #Replaced
pset = gp.PrimitiveSetTyped("MAIN", [], EXPR)

# pset = add_operators_base(pset) #Replaced
"""基础算子"""
# 无法给一个算子定义多种类型，只好定义多个不同名算子，之后通过helper.py中的convert_inverse_prim修正
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fadd')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fsub')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fmul')
pset.addPrimitive(dummy, [EXPR, EXPR], EXPR, name='fdiv')

# pset.addPrimitive(dummy, [EXPR, int], EXPR, name='iadd')
# pset.addPrimitive(dummy, [EXPR, int], EXPR, name='isub')
# pset.addPrimitive(dummy, [EXPR, int], EXPR, name='imul')
# pset.addPrimitive(dummy, [EXPR, int], EXPR, name='idiv')

# add_unary_ops(pset) 用下面的代码代替
for func in unary_funcs:
  pset.addPrimitive(dummy, [EXPR], EXPR, name=func)

# add_unary_rolling_ops(pset) 用下面的代码代替
for func in ts_rolling_funcs:
  pset.addPrimitive(dummy, [EXPR, int], EXPR, name=func)

# add_period_ops(pset) #Replaced
# add_binary_ops(pset) #Replaced
# add_binary_rolling_ops(pset) #Replaced


pset.addEphemeralConstant('_random_int_', _random_int_, int)
pset.addTerminal(1, EXPR, name=terminal)
# pset.addTerminal(1, EXPR, name='high')
# pset.addTerminal(1, EXPR, name='low')
# pset.addTerminal(1, EXPR, name='close')
# pset.addTerminal(1, EXPR, name='volume')

# toolbox.register("expr", gp.genHalfAndHalf, pset=pset, min_=1, max_=5)
toolbox.register("expr", gp.genFull, pset=pset, min_=1, max_=5)
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



toolbox.decorate("mate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))
toolbox.decorate("mutate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))

# 这里定义初始化的因子数，可以得行修改
print('开始生成因子...')
# 使用与mu相同的种群大小
# pop = toolbox.population(10)
pop = toolbox.population(n=150)  # type: ignore
for p in pop:
    pp = stringify_for_sympy_with_filter(p, terminal)
    print(pp)

hof = tools.HallOfFame(10)
# 只统计一个指标更清晰
stats = tools.Statistics(lambda ind: ind.fitness.values)
# 打补丁后，名人堂可以用nan了，如果全nan会报警
stats.register("avg", np.nanmean, axis=0)
stats.register("std", np.nanstd, axis=0)
stats.register("min", np.nanmin, axis=0)
stats.register("max", np.nanmax, axis=0)

# 使用修改版的eaMuPlusLambda
try:
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

    # 安全地打印logbook
    print('=' * 60)
    print("print logbook \n")
    if logbook and hasattr(logbook, 'select') and logbook.select:
        print(logbook)
    else:
        print("Logbook is empty or not properly populated.")
    print('=' * 60)
except Exception as e:
    print(f"Error during evolution: {e}")
    # 检查个体是否有适应度值
    for ind in population:
        if not hasattr(ind, 'fitness') or ind.fitness is None:
            print(f"Individual without fitness: {stringify_for_sympy_with_filter(ind, terminal)}")

print('=' * 60)

print("Hall of Fame:\n")
print(hof)
print('*' * 60)

print("print_population():\n")
def print_population(ppl):
  for _p in ppl:
    expr = stringify_for_sympy_with_filter(_p, terminal)
    print(expr, _p.fitness)


print_population(hof)
print('+' * 60)

# 添加打印所有生成的表达式
print("All generated expressions:")
print('-' * 40)
# 添加打印所有生成的表达式到CSV的功能
all_expressions = []
for i, ind in enumerate(pop):
    expr = stringify_for_sympy_with_filter(ind, terminal)
    print(f"{i+1:3d}: {expr}")
    # 收集所有表达式用于保存到CSV
    all_expressions.append({
        'index': i+1,
        'expression': expr
    })

# 保存所有生成的表达式到CSV文件
if all_expressions:
    df_all_expressions = pd.DataFrame(all_expressions)

    # 确保输出目录存在
    output_dir = 'results'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 保存到CSV文件
    all_output_file = os.path.join(output_dir, 'all_generated_expressions.csv')
    df_all_expressions.to_csv(all_output_file, index=False)
    print(f"所有生成的表达式已保存到: {all_output_file}")

# 保存最终结果到CSV文件
# 创建一个DataFrame来存储结果
results = []
for ind in hof:
    expr = stringify_for_sympy_with_filter(ind, terminal)
    # 确保个体有有效的适应度值
    if hasattr(ind, 'fitness') and ind.fitness is not None and len(ind.fitness.values) > 0:
        fitness = ind.fitness.values[0]
        results.append({
            'expression': expr,
            'fitness': fitness
        })

# 转换为DataFrame并保存
if results:  # 确保有结果可以保存
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('fitness', ascending=False)  # 按适应度降序排序

    # 确保输出目录存在
    output_dir = 'results'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 保存到CSV文件
    output_file = os.path.join(output_dir, 'factor_results.csv')
    df_results.to_csv(output_file, index=False)
    print(f"结果已保存到: {output_file}")

    # 打印前10个最佳因子
    print("\n前10个最佳因子:")
    print(df_results.head(10))
else:
    print("没有找到有效的因子结果，无法保存。")
