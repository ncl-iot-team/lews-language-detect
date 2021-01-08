import os
from procstream import StreamProcessMicroService
import os
import langdetect
import logging as logger

config = {"MODULE_NAME": os.environ.get('MODULE_NAME', 'LEWS_LANG_DETECT'),
          "CONSUMER_GROUP": os.environ.get("CONSUMER_GROUP", "LEWS_LANG_DETECT_CG")}

class StreamProcessLanguageDetectorService(StreamProcessMicroService):
    # def verify_env(self):
    #     print("Verify env derived")
    #     super().verify_env()

    def process_message(self, message):
        payload = message.value
        try:
            detected_language = langdetect.detect(payload.get("text"))
            payload["lews_meta_detected_lang"] = detected_language
        except:
            logger.error("Cannot detect language")
            payload["lews_meta_detected_lang"] = None
        print(payload)
        return payload


def main():
    k_service = StreamProcessLanguageDetectorService(config)
    k_service.start_service()


if __name__ == "__main__":
    main()

#
