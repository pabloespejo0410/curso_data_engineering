WITH source_data AS (

    SELECT
        -- Clave Primaria
        ADDRESS_ID, 
        
        -- Datos de Dirección
        ZIPCODE,
        COUNTRY,
        ADDRESS,
        STATE,

        _FIVETRAN_DELETED AS is_deleted, -- 2. Renombrar la bandera booleana a un nombre más intuitivo
        _FIVETRAN_SYNCED AS loaded_at     -- 3. Renombrar la marca de tiempo a un nombre estándar

    FROM 
        {{ source('SQL_SERVER_DBO', 'ADDRESSES') }} -- 4. Referencia al source definido en el YML

)

SELECT * FROM source_data



