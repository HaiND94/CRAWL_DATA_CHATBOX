import logging

# Config logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log_file.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def remove_duplicates(x):
  return list(dict.fromkeys(x))


def get_data(list_texts, txt_check):
    """
    To get answer data
    """
    
    max_idx = 0
    if len(list_texts) < 2:
        logger.error("list texts input is wrong must be have len > 2")
        return False

    # Find index of str which need to remove
    for idx, txt in enumerate(list_texts):
        if txt_check in txt:
            max_idx = idx

    new_list = list_texts[max_idx+1:-2]
    new_str = " ".join(new_list)
    
    del max_idx, new_list

    return new_str