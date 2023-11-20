from io import BytesIO
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
import zipfile
import os
import logging

def zipfiles(folder_name,filenames):
    logging.info(f"zipfiles {filenames}")
    zip_io = BytesIO()
    with zipfile.ZipFile(zip_io, mode='w', compression=zipfile.ZIP_DEFLATED) as temp_zip:
        with os.scandir(folder_name) as entries:
            for entry in entries:
                if os.path.isdir(folder_name+ entry.name):
                    continue
                zip_path = os.path.join(folder_name, entry.name)
                try:
                    temp_zip.write(folder_name+entry.name, zip_path)
                except Exception as e:
                    logging.info(f"error {e}")

    return StreamingResponse(
        iter([zip_io.getvalue()]), 
        media_type="application/x-zip-compressed", 
        headers = { "Content-Disposition": f"attachment; filename=images.zip"}
    )

def get_files(folder_name,filenames)->dict:
    docs_dict = {}
    for filename in filenames:
        file_path = os.path.join(folder_name, filename+".py")
        if not os.path.exists(file_path) or os.path.isdir(file_path)  :
            continue
        with open(file_path) as file:
            file_content = file.read()
            docs_dict[filename] = file_content
            file.close()
    return docs_dict

def get_all_files(folder_name)->dict:
    docs_dict = {}
    for file in os.listdir(folder_name):
        file_path = os.path.join(folder_name, file)
        filename,extension=os.path.splitext(file)
        if os.path.isdir(file_path) or extension!=".py" :
            continue
        with open(file_path) as file:
            file_content = file.read()
            docs_dict[filename] = file_content
            file.close()
    return docs_dict

# def fileContent(folder_name,file_name)->PlainTextResponse:
#     with open(folder_name+file_name+".py", "r") as file:
#             file_content = file.read()
#     return PlainTextResponse(file_content, media_type="text/plain")