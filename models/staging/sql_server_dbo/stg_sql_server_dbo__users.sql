WITH source_data AS (

    SELECT

        -- CLAVE SUBROGADA

        MD5(CAST(USER_ID AS VARCHAR)) AS user_sk,
        CAST(USER_ID AS VARCHAR) AS user_id,
        
        -- CLAVE FORÁNEA

        MD5(CAST(ADDRESS_ID AS VARCHAR)) AS address_sk,
        
        -- ATRIBUTOS

        CAST(FIRST_NAME AS VARCHAR) AS first_name,
        CAST(LAST_NAME AS VARCHAR) AS last_name,

        -- CONCATENAR NOMBRE COMPLETO

        TRIM(first_name || ' ' || last_name) AS full_name,
        CAST(EMAIL AS VARCHAR) AS email,
        CAST(PHONE_NUMBER AS VARCHAR) AS phone_number,
        
        -- METADATOS TIEMPO (CREADO, ACTUALIZADO)

        CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at,
        CONVERT_TIMEZONE('UTC', UPDATED_AT) AS updated_at,
        
        -- METADATOS FIVETRAN
        
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at

    FROM 
        {{ source('SQL_SERVER_DBO', 'USERS') }} 

    WHERE USER_ID IS NOT NULL 

)

SELECT * FROM source_data