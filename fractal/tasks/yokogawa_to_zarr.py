import os
import re
from glob import glob

import dask.array as da
import numpy as np
from dask import delayed
from skimage.io import imread


def sort_fun(s):

    """
    sort_fun takes a string (filename of a yokogawa images),
    extract site and z-index metadata and returns them as a list.

    :param s: filename
    :type s: str

    """

    site = re.findall(r"F(.*)L", s)[0]
    zind = re.findall(r"Z(.*)C", s)[0]
    return [site, zind]


def yokogawa_to_zarr(
    in_path,
    out_path,
    zarrurl,
    delete_in,
    rows,
    cols,
    ext,
    chl_list,
    coarsening_factor=2,
    num_levels=5,
):

    """
    Convert Yokogawa output (png, tif) to zarr file

    :param in_path: directory containing the input files
    :type in_path: str
    :param out_path: directory containing the output files
    :type out_path: str
    :param zarrurl: structure of the zarr folder
    :type zarrurl: str
    :param delete_in: delete input files, and folder if empty
    :type delete_in: str
    :param rows: number of rows of the plate
    :type rows: int
    :param cols: number of columns of the plate
    :type cols: int
    :param ext: source images extension
    :type ext: str
    :param chl_list: list of the channels
    :type chl_list: list

    :param coarsening_factor: .... FIXME (default=2)
    :type int
    :param num_levels: .... FIXME (default=2)
    :type int

    """

    r = zarrurl.split("/")[1]
    c = zarrurl.split("/")[2]

    lazy_imread = delayed(imread)
    fc_list = {level: [] for level in range(num_levels)}

    print(chl_list)

    for ch in chl_list:

        l_rows = []
        all_rows = []

        filenames = sorted(
            glob(in_path + f"*_{r+c}_*C{ch}." + ext), key=sort_fun
        )
        print(in_path + f"*_{r+c}_*C{ch}." + ext)
        max_z = max([re.findall(r"Z(.*)C", s)[0] for s in filenames])

        sample = imread(filenames[0])

        s = 0
        e = int(max_z)

        for ro in range(int(rows)):
            cell = []

            for co in range(int(cols)):
                lazy_arrays = [lazy_imread(fn) for fn in filenames[s:e]]
                s += int(max_z)
                e += int(max_z)
                dask_arrays = [
                    da.from_delayed(
                        delayed_reader, shape=sample.shape, dtype=sample.dtype
                    )
                    for delayed_reader in lazy_arrays
                ]
                z_stack = da.stack(dask_arrays, axis=0)
                cell.append(z_stack)
            l_rows = da.block(cell)
            all_rows.append(l_rows)

        coarsening = {0: coarsening_factor, 1: coarsening_factor}

        f_matrices = {}
        for level in range(num_levels):
            if level == 0:
                f_matrices[level] = da.concatenate(all_rows, axis=1)
            else:
                f_matrices[level] = [
                    da.coarsen(np.min, x, coarsening, trim_excess=True)
                    for x in f_matrices[level - 1]
                ]
            fc_list[level].append(f_matrices[level])

    tmp_lvl = [da.stack(fc_list[level], axis=0) for level in range(num_levels)]

    shape_list = []
    for i, level in enumerate(tmp_lvl):
        level.to_zarr(out_path + zarrurl + f"{i}/", dimension_separator="/")
        shape_list.append(level.shape)

    if delete_in == "True":
        for f in filenames:
            try:
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))
    return shape_list


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(prog="Yokogawa_to_zarr")

    parser.add_argument(
        "-i", "--in_path", help="directory containing the input files"
    )

    parser.add_argument(
        "-o", "--out_path", help="directory containing the output files"
    )

    parser.add_argument(
        "-z",
        "--zarrurl",
        help="structure of the zarr folder",
    )

    parser.add_argument(
        "-d",
        "--delete_in",
        help="Delete input files and folder",
    )

    parser.add_argument(
        "-r",
        "--rows",
        help="Number of rows of final image",
    )

    parser.add_argument(
        "-c",
        "--cols",
        help="Number of columns of final image",
    )

    parser.add_argument(
        "-e",
        "--ext",
        help="source images extension",
    )

    parser.add_argument(
        "-C",
        "--chl_list",
        nargs="+",
        help="list of channels ",
    )

    args = parser.parse_args()

    yokogawa_to_zarr(
        args.in_path,
        args.out_path,
        args.zarrurl,
        args.delete_in,
        args.rows,
        args.cols,
        args.ext,
        args.chl_list,
    )
