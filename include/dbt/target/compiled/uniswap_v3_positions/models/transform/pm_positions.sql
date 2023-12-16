SELECT 
    pl.owner,
    pl.pool,
    pc.token_0,
    pc.token_1,
    pl.tick_lower,
    pl.tick_upper,
    pc.tick,
    pc.fee,
    `spock-main.uniswap_v3_positions.decode_position`(
        1,
        pc.token_0,
        pc.token_1,
        pc.decimals_0,
        pc.decimals_1,
        pc.fee,
        pc.sqrt_price_x96,
        pc.liquidity,
        pc.tick,
        pl.liquidity,
        pl.tick_lower,
        pl.tick_upper
    ) AS amounts
FROM 
    `spock-main`.`uniswap_v3_positions`.`pm_liquidities` AS pl
INNER JOIN 
    `spock-main`.`uniswap_v3_positions`.`positions_calculation_data` AS pc
ON 
    pl.pool = pc.pool
WHERE
    pc.decimals_0 IS NOT NULL
    AND CAST(pc.decimals_0 AS INT) > 0
    AND pc.decimals_1 IS NOT NULL
    AND CAST(pc.decimals_1 AS INT) > 0
    AND pl.liquidity > 0