import argparse
import os
import pandas
import re

EXPECTED_KEYFILE_COLUMNS = [
    'GDW_ID',
    'GRAND_ID',
    'DAM_NAME',
    'COUNTRY',
    'YEAR_',
    'AREA_SKM',
    'CAP_MCM',
    'MAIN_USE',
    'GDW_NUM',
    ]


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
            'Path to csv that contains column names of ' +
            ', '.join(EXPECTED_KEYFILE_COLUMNS)))
    parser.add_argument(
        'dam_fullness_path', help=(
            'Path to csv that contains a date column followd by N id columns '
            'that correspond to the GWD_ID in the `dam_definition_path` '
            'table.'))
    parser.add_argument(
        'start_month_day', help='Yearly day to start the historical analysis.',
        type=valid_month_date)
    parser.add_argument(
        'end_month_day', help='Yearly day to end the historical analysis.',
        type=valid_month_date)
    parser.add_argument(
        'analysis_year_month_day', help='YYYY-MM-DD to do the analysis on.',
        type=valid_year_month_date)
    parser.add_argument(
        'target_table_path', help='Path to target table, will not overwrite')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    if os.path.exists(args.target_table_path) and not args.force:
        raise ValueError(
            f'{args.target_table_path} already exists, will not overwrite')
    dam_definition_table = pandas.read_csv(
        args.dam_definition_path, usecols=EXPECTED_KEYFILE_COLUMNS)
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

    result_df = pandas.DataFrame(
        columns=['analysis_date'] + list(dam_definition_table.columns)+[
            'res.m', 'perc.mean', 'perc.cap'])
    for dam_id in dam_fullness_table:
        if not is_int(dam_id):
            continue
        dam_id = int(dam_id)
        if not dam_definition_table['GDW_ID'].isin([dam_id]).any():
            continue
        dam_fullness_col = dam_fullness_table[[str(dam_id)]]
        analysis_day = pandas.Timestamp(args.analysis_year_month_day)
        closest_date_index = (
            (dam_fullness_table['Dates'] - analysis_day).abs()).idxmin()
        analysis_day_surface_area = dam_fullness_table.loc[
            closest_date_index, str(dam_id)]
        dam_info = dam_definition_table[
            dam_definition_table['GDW_ID'] == dam_id].to_dict(
                orient='records')[0]
        print(dam_info)
        print(dam_fullness_col.mean())
        print(dam_info['AREA_SKM'])
        dam_info['res.m'] = dam_fullness_col.mean()
        dam_info['perc.mean'] = dam_fullness_col.mean() / dam_info['AREA_SKM'] * 100
        dam_info['perc.cap'] = analysis_day_surface_area / dam_info['AREA_SKM'] * 100
        print(dam_info)
        #dam_info['analysis_date'] = args.analysis_year_month_day
        local_dam_fullness_table = dam_fullness_table[
            dam_fullness_table['Dates'].dt.year >= dam_info['YEAR_']]
        result_df = pandas.concat([pandas.DataFrame(dam_info), result_df], ignore_index=True)
    result_df['analysis_date'] = args.analysis_year_month_day
    print(result_df)
    result_df.to_csv(args.target_table_path, index=False)


if __name__ == '__main__':
    main()
