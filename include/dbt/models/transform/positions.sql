SELECT 
    l.owner,
    l.pool,
    p.token_0,
    p.token_1,
    l.tick_lower,
    l.tick_upper,
    p.tick,
    p.fee,
    `spock-main.uniswap_v3_positions.decode_position`(
        1,
        p.token_0,
        p.token_1,
        p.decimals_0,
        p.decimals_1,
        p.fee,
        p.sqrt_price_x96,
        p.liquidity,
        p.tick,
        l.liquidity,
        l.tick_lower,
        l.tick_upper
    ) AS amounts
FROM 
    {{ ref('liquidities') }} AS l
INNER JOIN 
    {{ ref('positions_calculation_data') }} AS p 
ON 
    l.pool = p.pool
WHERE
    p.decimals_0 IS NOT NULL
    AND CAST(p.decimals_0 AS INT) > 0
    AND p.decimals_1 IS NOT NULL
    AND CAST(p.decimals_1 AS INT) > 0
    AND l.liquidity > 0