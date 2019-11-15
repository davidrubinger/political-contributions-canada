create_contributions = f"""
    drop table if exists contributions;
    create table contributions (
        received_date text,
        contributor_first_name text,
        contributor_middle_initial text,
        contributor_last_name text,
        contributor_city text,
        contributor_province_code text,
        contributor_postal_code text,
        contributor_type text,
        recipient_first_name text,
        recipient_middle_initial text,
        recipient_last_name text,
        recipient_entity text,
        recipient_party text,
        electoral_district text,
        electoral_event text,
        fiscal_election_date text,
        monetary_amount text,
        non_monetary_amount text,
        report_id text,
        report_name text,
        report_part_number text,
        report_part_name text
    );
    """
