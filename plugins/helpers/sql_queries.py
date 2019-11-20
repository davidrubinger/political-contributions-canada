create_contributions = f"""
    drop table if exists contributions;
    create table contributions (
        year int,
        contributor_province_code text,
        electoral_district text,
        recipient_party text,
        monetary_amount numeric
    );
    """

create_population = f"""
    drop table if exists population;
    create table population (
        reference_date text,
        province_code char(2) not null,
        population text
    );
    """
