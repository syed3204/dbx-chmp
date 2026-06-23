# Databricks notebook source
import ipywidgets as widgets
from IPython.display import display, Image
import os

# Resolve the Images directory path
# notebookPath() returns the CALLER's path (e.g., .../01 - Interact with 01-Data Ingestion)
# The caller is always in the course folder, so strip the notebook name to get the course folder
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_course_folder = _nb_path.rsplit("/", 1)[0]
_images_dir = f"/Workspace{_course_folder}/Includes/Images"
print(f"Images directory: {_images_dir}")


def upload_evidence(evidence_name):
    """
    Display a drag-and-drop upload widget that saves the uploaded file
    with a fixed name to the Images/ folder.

    Args:
        evidence_name: The fixed filename to save as (e.g., "evidence-01.png")
    """
    save_path = f"{_images_dir}/{evidence_name}"

    uploader = widgets.FileUpload(accept='.png,.jpg,.jpeg,.gif', multiple=False, description='Upload Screenshot')

    def _on_upload(change):
        for name, file_info in uploader.value.items():
            with open(save_path, 'wb') as f:
                f.write(file_info['content'])
            print(f"Saved as Includes/Images/{evidence_name} -- now run the next cell to display.")

    uploader.observe(_on_upload, names='value')
    display(uploader)


def show_evidence(evidence_name, width=800):
    """
    Display a previously uploaded evidence image.
    This output survives HTML export.

    Args:
        evidence_name: The filename to display (e.g., "evidence-01.png")
        width: Display width in pixels (default 800)
    """
    save_path = f"{_images_dir}/{evidence_name}"
    if os.path.exists(save_path):
        display(Image(filename=save_path, width=width))
    else:
        print(f"No screenshot found for '{evidence_name}'.")
        print(f"Upload your screenshot in the cell above first.")


# Legacy function for backward compatibility
def show_workspace_image(relative_path, figsize=(12, 8)):
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    full_path = f"{_images_dir}/{os.path.basename(relative_path)}"
    if os.path.exists(full_path):
        img = mpimg.imread(full_path)
        plt.figure(figsize=figsize)
        plt.imshow(img)
        plt.axis('off')
        plt.show()
    else:
        print(f"Image not found: {relative_path}")
