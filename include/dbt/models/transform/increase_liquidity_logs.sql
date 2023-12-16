WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_positions.decode_increase_liquidity_log`(data, topics) AS decoded_data
  FROM 
    {{ ref('filtered_logs') }}
  WHERE 
    -- INCREASE_LIQUIDITY
    topics[0] = '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f'
)

SELECT 
    decoded_data.tokenId AS `token_id`,
    decoded_data.liquidity AS `liquidity`,
FROM 
    decoded_log
