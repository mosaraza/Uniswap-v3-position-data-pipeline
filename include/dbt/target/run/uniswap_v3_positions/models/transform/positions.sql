
  
    

    create or replace table `spock-main`.`uniswap_v3_positions`.`positions`
    
    

    OPTIONS()
    as (
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
    `spock-main`.`uniswap_v3_positions`.`liquidities` AS l
INNER JOIN 
    `spock-main`.`uniswap_v3_positions`.`positions_calculation_data` AS p 
ON 
    l.pool = p.pool
WHERE
    p.decimals_0 IS NOT NULL
    AND p.decimals_1 IS NOT NULL
    AND l.liquidity > 0
    );
  