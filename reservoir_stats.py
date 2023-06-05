import argparse
import pandas

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
        'target_table_path', help='Path to target table, will not overwrite')
    args = parser.parse_args()
    if os.path.exists(args.target_table_path):
        raise ValueError(
            f'{args.target_table_path} already exists, will not overwrite')
    dam_definition_table = pandas.read_csv(
        args.dam_definition_path, usecols=EXPECTED_KEYFILE_COLUMNS)
    dam_fullness_table = pandas.read_csv(args.dam_fullness_path)
    dam_fullness_table['Dates'] = pandas.to_datetime(
        dam_fullness_table['Dates'])

    result_df = pandas.DataFrame(
        columns=list(dam_definition_table.columns)+[
            'res.m', 'perc.mean', 'perc.cap'])
    for col in dam_fullness_table:
        if not is_int(col):
            continue
        col = int(col)
        if not dam_definition_table['GDW_ID'].isin([col]).any():
            continue
        dam_info = dam_definition_table[
            dam_definition_table['GDW_ID'] == int(col)].copy()
        dam_info['res.m'] = -1
        dam_info['perc.mean'] = -1
        dam_info['perc.cap'] = -1
        result_df = pandas.concat([dam_info, result_df], ignore_index=True)
    print(result_df)


if __name__ == '__main__':
    main()
