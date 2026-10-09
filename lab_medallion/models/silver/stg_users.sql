with source as (
    select * from {{ source('bronze', 'users') }}
),
first_orders as (
    select 
        cast(user_uuid as uuid) as user_id,
        min(cast(date as timestamptz)) as first_order_at
    from {{ source('bronze', 'orders') }}
    group by 1
),
typed as (
    select
        cast(u.uuid as uuid) as user_id,
        trim(u.username) as username,
        -- Exercice 1: username_normalized (minuscules + espaces retirés)
        lower(trim(u.username)) as username_normalized,
        trim(u.name) as name,
        upper(trim(u.sex)) as sex,
        lower(trim(u.mail)) as email,
        cast(u.birthdate as date) as raw_birthdate,
        f.first_order_at,
        split_part(u.address, chr(10), 1) as street,
        split_part(u.address, chr(10), 2) as address_line_2
    from source u
    left join first_orders f on cast(u.uuid as uuid) = f.user_id
),
validated as (
    select
        user_id,
        username,
        username_normalized,
        name,
        sex,
        email,
        raw_birthdate,
        -- Exercice 3: indicateur de validité
        case 
            when first_order_at is null or raw_birthdate <= cast(first_order_at as date) then true
            else false
        end as birthdate_is_valid,
        -- Exercice 3: mise à NULL si la date de naissance est postérieure à la 1re commande
        case 
            when first_order_at is null or raw_birthdate <= cast(first_order_at as date) then raw_birthdate
            else null
        end as birthdate,
        street,
        nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
        regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
        regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
    from typed
)
select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    birthdate,
    birthdate_is_valid,
    street,
    city,
    state,
    zip_code
from validated
