"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import json
from glob import glob

import zarr


def replicate_zarr_structure_mip(zarrurl):
    """
    Duplicate an input zarr structure to a new path, adapting it to host a
    maximum-intensity projection (that is, with a single Z layer).

    :param zarrurl: structure of the input zarr folder
    :type zarrurl: str
    """

    # Sanitize and check input zarr path
    if not zarrurl.endswith("/"):
        zarrurl += "/"
    if not zarrurl.endswith(".zarr/"):
        raise Exception(
            "Error in replicate_zarr_structure, "
            f"zarrurl={zarrurl} does not end with .zarr/"
        )

    # Filename for new zarr file
    zarrurl_mip = zarrurl.replace(".zarr/", "_mip.zarr/")

    # Identify properties of input zarr file
    well_rows_columns = sorted(
        [rc.split("/")[-2:] for rc in glob(zarrurl + "*/*")]
    )

    group_plate = zarr.group(zarrurl_mip)
    plate = zarrurl.replace(".zarr/", "").split("/")[-1]
    group_plate.attrs["plate"] = {
        "acquisitions": [{"id": 0, "name": plate}],
        # takes unique cols from (row,col) tuples
        "columns": sorted(
            [
                {"name": u_col}
                for u_col in set(
                    [
                        well_row_column[1]
                        for well_row_column in well_rows_columns
                    ]
                )
            ],
            key=lambda key: key["name"],
        ),
        # takes unique rows from (row,col) tuples
        "rows": sorted(
            [
                {"name": u_row}
                for u_row in set(
                    [
                        well_row_column[0]
                        for well_row_column in well_rows_columns
                    ]
                )
            ],
            key=lambda key: key["name"],
        ),
        "wells": [
            {
                "path": well_row_column[0] + "/" + well_row_column[1],
            }
            for well_row_column in well_rows_columns
        ],
    }

    for row, column in well_rows_columns:

        # Find sites in COL/ROW/.zattrs
        path_well_zattrs = zarrurl + f"{row}/{column}/.zattrs"
        with open(path_well_zattrs) as well_zattrs_file:
            well_zattrs = json.load(well_zattrs_file)
        well_images = well_zattrs["well"]["images"]
        list_FOVs = sorted([img["path"] for img in well_images])

        # Create well group
        group_well = group_plate.create_group(f"{row}/{column}/")
        group_well.attrs["well"] = {
            "images": well_images,
            "version": "0.3",
        }

        for FOV in list_FOVs:
            # Create FOV group
            group_FOV = group_well.create_group(f"{FOV}/")  # noqa: F841

            # Copy .zattrs file at the COL/ROW/SITE level
            path_FOV_zattrs = zarrurl + f"{row}/{column}/{FOV}/.zattrs"
            with open(path_FOV_zattrs) as FOV_zattrs_file:
                FOV_zattrs = json.load(FOV_zattrs_file)
            for key in FOV_zattrs.keys():
                group_FOV.attrs[key] = FOV_zattrs[key]


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(prog="FIXME")
    parser.add_argument("-z", "--zarrurl", help="zarr url", required=True)
    args = parser.parse_args()
    replicate_zarr_structure_mip(args.zarrurl)
