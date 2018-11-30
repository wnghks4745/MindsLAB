#!/usr/bin/python
# -*- coding: utf-8 -*-


def main():
    category_rank = {
        'fir': {'aa': 1, 'bb': 3, 'cc': 2},
        'sec': {'cc': 1, 'dd': 2},
        'thi': {'ee': 1, 'ff': 3, 'gg': 2, 'hh': 4}
    }
    tar_category = {
        'bb': 1,
        'cc': 1,
        'dd': 1,
        'gg': 1,
    }
    for key, value in category_rank.items():
        temp_cat = ''
        temp_rank = 0
        print tar_category
        for cat in tar_category.keys():
            if cat in value:
                rank = value[cat]
                if temp_rank == 0:
                    temp_cat = cat
                    temp_rank = rank
                elif temp_rank > rank:
                    del tar_category[temp_cat]
                else:
                    del tar_category[cat]

    print tar_category

if __name__ == '__main__':
    main()