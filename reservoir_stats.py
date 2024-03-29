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
        'start_month_day',
        help='Yearly day to start the seasonal historical analysis.',
        type=valid_month_date)
    parser.add_argument(
        'end_month_day',
        help='Yearly day to end the seasonal historical analysis.',
        type=valid_month_date)
    parser.add_argument(
        'historical_year_range',
        help='historical start and end year range in YYYY-YYYY')
    parser.add_argument(
        'analysis_year_month_day', help='YYYY-MM-DD to do the analysis on.',
        type=valid_year_month_date)
    parser.add_argument(
        'target_table_means_path',
        help='Path to target table, will not overwrite')
    parser.add_argument(
        '--target_table_daily_fullness_path', help=(
            'Path to target table that shows fullness of all dams by day, '
            'will not overwrite'))
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    for table_path in [args.target_table_means_path,
                       args.target_table_daily_fullness_path]:
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

    historic_start_year, historic_end_year = [
        int(year) for year in args.historical_year_range.split('-')]
    dam_fullness_table = pandas.read_csv(args.dam_fullness_path)
    dam_fullness_table['Dates'] = pandas.to_datetime(
        dam_fullness_table['Dates'])

    start_month, start_day = [int(v) for v in args.start_month_day.split('-')]
    end_month, end_day = [int(v) for v in args.end_month_day.split('-')]

    # fitler out by season
    dam_fullness_table = (
        dam_fullness_table[
            ((dam_fullness_table['Dates'].dt.month >= start_month) &
             (dam_fullness_table['Dates'].dt.day >= start_day)) &
            ((dam_fullness_table['Dates'].dt.month <= end_month) &
             (dam_fullness_table['Dates'].dt.day <= end_day))])

    result_mean_fullness_df = pandas.DataFrame(
        columns=['analysis_date'] + list(dam_definition_table.columns)+[
            'res.m', 'perc.mean', 'perc.cap'])
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

    for dam_id in dam_fullness_table:
        if not is_int(dam_id):
            continue
        dam_id = int(dam_id)
        if not dam_definition_table[DAM_COL_ID].isin([dam_id]).any():
            continue
        analysis_day = pandas.Timestamp(args.analysis_year_month_day)
        closest_date_index = (
            (dam_fullness_table['Dates'] - analysis_day).abs()).idxmin()
        analysis_day_surface_area = dam_fullness_table.loc[
            closest_date_index, str(dam_id)]
        dam_info = dam_definition_table[
            dam_definition_table[DAM_COL_ID] == dam_id].to_dict(
                orient='records')[0]
        local_dam_fullness_table = dam_fullness_table[
            (dam_fullness_table['Dates'].dt.year >= dam_info['YEAR_']) &
            (dam_fullness_table['Dates'].dt.year >= historic_start_year) &
            (dam_fullness_table['Dates'].dt.year <= historic_end_year)]
        dam_fullness_col = local_dam_fullness_table[[str(dam_id)]]

        dam_info['res.m'] = dam_fullness_col.mean()
        dam_info['perc.mean'] = (
            analysis_day_surface_area / dam_fullness_col.mean() * 100)
        dam_info['perc.cap'] = (
            analysis_day_surface_area / dam_info['AREA_SKM'] * 100)
        result_mean_fullness_df = pandas.concat(
            [pandas.DataFrame(dam_info), result_mean_fullness_df],
            ignore_index=True)
    result_mean_fullness_df['analysis_date'] = args.analysis_year_month_day
    result_mean_fullness_df.to_csv(args.target_table_means_path, index=False)
    print(
        f"Dam 'mean fullness' table written to {args.target_table_means_path}")


if __name__ == '__main__':
    main()
