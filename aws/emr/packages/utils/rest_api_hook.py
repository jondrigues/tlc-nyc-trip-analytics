import requests
import time
from packages.utils.logger import Logger


class RestApiHook:
    def __init__(self):
        self._logger = Logger()

    def __validate_status_code(self, method, response, retries, stream):
        if response.status_code in (200, 201):
            return response
        elif response.status_code in (500, 502, 504) and retries > 0:
            self._logger.info(
                f"Response status_code {response.status_code}. Sleeping 30 seconds then retrying..."
            )
            time.sleep(30)
            return self.__run_request(
                endpoint=response.url,
                headers=response.headers,
                method=method,
                stream=stream,
                retries=retries - 1,
            )
        else:
            raise Exception(
                "Request returned status_code {}. Content: {}".format(
                    response.status_code, response.text
                )
            )

    def __run_request(self, method, endpoint, headers, stream, data=None, json=None, retries=0):
        if method == "get":
            response = requests.get(url=endpoint, headers=headers, params=data, stream=stream)
        else:
            raise Exception(f"Method {method} not implemented yet.")

        return self.__validate_status_code(
            method=method, response=response, retries=retries, stream=stream
        )

    def __request_output(self, request, output_type, encoding):
        request.encoding = encoding
        if output_type == "json":
            return request.json()
        elif output_type == "text":
            return request.text
        elif output_type == "raw":
            request.raw.decode_content = True
            return request.raw
        else:
            self._logger.error("Error retrieving request result.")
            raise Exception(f"No such {output_type} output_type implemented yet.")

    def get(self, endpoint, headers, output_type="raw", retries=0, stream=True, encoding=None):
        request = self.__run_request(
            "get", endpoint=endpoint, headers=headers, retries=retries, stream=stream
        )
        return self.__request_output(request, output_type, encoding=encoding)
