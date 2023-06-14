from gymnasium.envs.registration import register

register(
    id='udo_optimization-v0',
    entry_point='udo.udo_optimization.envs:UDOEnv',
)
