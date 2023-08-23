import argparse
import os
import pandas
import re

DAM_COL_ID = 'GDW_ID'


def is_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def valid_month_date(date_string):
    pattern = r'^\d{2}-\d{2}$'
    if not re.match(pattern, date_string):
        raise argparse.ArgumentTypeError(
            "Invalid date format. Expected MM-DD.")
    return date_string


def valid_year_month_date(date_string):
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pattern, date_string):
        raise argparse.ArgumentTypeError(
            "Invalid date format. Expected YYYY-MM-DD.")
    return date_string


def main():
    parser = argparse.ArgumentParser(description=(
        'Process reservoir fullness stats with build dates to generate '
        'fullness statistics.'))
    parser.add_argument(
        'dam_definition_path', help=(
            'Path to csv that contains at least a column with a unique id '
            f'of {DAM_COL_ID}.'))
    parser.add_argument(
        'dam_fullness_path', help=(
            'Path to csv that contains a date column followd by N id columns '
            f'that correspond to the {DAM_COL_ID} in the '
            '`dam_definition_path` table.'))
    parser.add_argument(
        'target_table_daily_fullness_path', help=(
            'Path to target table that shows fullness of all dams by day, '
            'will not overwrite'))
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    for table_path in [args.target_table_daily_fullness_path]:
        if table_path is None:
            continue
        if os.path.exists(table_path) and not args.force:
            raise ValueError(
                f'{table_path} already exists, will not overwrite')
    dam_definition_table = pandas.read_csv(
        args.dam_definition_path)
    if DAM_COL_ID not in dam_definition_table:
        raise ValueError(
            f"Expected a coulumn named {DAM_COL_ID} but one was not found in "
            "the table.")

    dam_fullness_table = pandas.read_csv(args.dam_fullness_path)
    dam_fullness_table['Dates'] = pandas.to_datetime(
        dam_fullness_table['Dates'])

    # 'current_km2_surface_area'
    if args.target_table_daily_fullness_path:
        merged_dfs = []
        target_table_all_fullness = pandas.DataFrame()
        for dam_id in dam_fullness_table:
            if not is_int(dam_id):
                continue
            dam_id = int(dam_id)
            if not dam_definition_table[DAM_COL_ID].isin([dam_id]).any():
                continue
            # Filter table A by the current DAM_COL_ID
            dam_id_str = str(dam_id)
            subset_a = dam_definition_table[
                dam_definition_table[DAM_COL_ID] == dam_id]
            subset_b = dam_fullness_table[[dam_id_str, 'Dates']]
            subset_b = subset_b.rename(
                columns={dam_id_str: 'current_km2_surface_area'})
            subset_b[DAM_COL_ID] = dam_id
            merged = pandas.merge(
                subset_a, subset_b, on=DAM_COL_ID, how='outer')
            merged_dfs.append(merged)

        target_table_all_fullness = pandas.concat(
            merged_dfs, ignore_index=True)
        target_table_all_fullness.to_csv(
            args.target_table_daily_fullness_path, index=False)
        print(
            f"Dam 'daily fullness' table written to "
            f"{args.target_table_daily_fullness_path}")

if __name__ == '__main__':
    main()
