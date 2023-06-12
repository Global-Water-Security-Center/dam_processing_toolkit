import argparse
import os
import pandas
import re

DAM_COL_ID = 'GWD_ID'


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
        args.dam_definition_path)
    if 'GWD_ID' not in dam_definition_table:
        raise ValueError(
            "Expected a coulumn named 'GWD_ID' but one was not found in "
            "the table.")
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
            dam_fullness_table['Dates'].dt.year >= dam_info['YEAR_']]
        dam_fullness_col = local_dam_fullness_table[[str(dam_id)]]

        print(dam_info)
        print(dam_fullness_col.mean())
        print(dam_info['AREA_SKM'])
        dam_info['res.m'] = dam_fullness_col.mean()
        dam_info['perc.mean'] = (
            dam_fullness_col.mean() / dam_info['AREA_SKM'] * 100)
        dam_info['perc.cap'] = (
            analysis_day_surface_area / dam_info['AREA_SKM'] * 100)
        print(dam_info)
        result_df = pandas.concat(
            [pandas.DataFrame(dam_info), result_df], ignore_index=True)
    result_df['analysis_date'] = args.analysis_year_month_day
    print(result_df)
    result_df.to_csv(args.target_table_path, index=False)


if __name__ == '__main__':
    main()
