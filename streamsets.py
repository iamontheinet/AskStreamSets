from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from ask_sdk_core.handler_input import HandlerInput
from ask_sdk_model import Response
from ask_sdk_model.ui import SimpleCard
from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractExceptionHandler
import requests
import json
import configparser

sb = SkillBuilder()

Config = configparser.ConfigParser()
Config.read("config.ini")

SCH_BASE_URL = Config.get('SCH','BaseURL')
SCH_USERNAME = Config.get('SCH','Username')
SCH_PASSWORD = Config.get('SCH','Password')

creds = {"userName": SCH_USERNAME, "password": SCH_PASSWORD}
auth = requests.post(SCH_BASE_URL + '/security/public-rest/v1/authentication/login', data=json.dumps(creds), headers={"X-Requested-By":"Alexa","Content-Type": "application/json"})
auth_token = auth.cookies['SS-SSO-LOGIN']

class StartJobIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("StartJobIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/jobrunner/rest/v1/jobs/byStatus?jobStatus=INACTIVE&orderBy=LAST_MODIFIED_ON&order=DESC&executorType=TRANSFORMER', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})
    jobs = json.loads(r.text)
    number_of_jobs = len(jobs)
    start_job = None
    slots = handler_input.request_envelope.request.intent.slots
    job_type = slots["Job"].value
    # print("===== slots =====")
    # print(slots)
    # print("=====")
    # print("===== job =====")
    # print(slots["Job"].name)
    # print(slots["Job"].value)
    # print("=====")

    if (number_of_jobs > 0):
        for job in jobs:
            if (job_type.lower() in job['name'].lower()):
                start_job = job
                print("===== Job about to start =====")
                print(job)
                print("=====")
                break

        if (start_job != None):
            print("Ok, you have a total of " + str(number_of_jobs) + " inactive " + job_type + " jobs. Starting the last modified job named " + start_job['name'])
            speech_text = "Ok, you have a total of " + str(number_of_jobs) + " inactive " + job_type + " jobs. Starting the last modified job named " + start_job['name']

            r = requests.post(SCH_BASE_URL + '/jobrunner/rest/v1/job/' + start_job['id'] + '/start', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})
            print("===== Job start status =====")
            print(json.loads(r.text))
            print("=====")
        else:
            print("You have a total of " + str(number_of_jobs) + " inactive jobs, but none of them are of type '" + job_type + ".'")
            speech_text = "You have a total of " + str(number_of_jobs) + " inactive jobs, but none of them are of type '" + job_type + ".'"
    else:
        speech_text = "You have a total of 0 inactive " + job_type + " jobs."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class StopJobIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("StopJobIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/jobrunner/rest/v1/jobs/byStatus?jobStatus=ACTIVE&orderBy=LAST_MODIFIED_ON&order=DESC&executorType=TRANSFORMER', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})
    jobs = json.loads(r.text)
    number_of_jobs = len(jobs)
    stop_job = None
    slots = handler_input.request_envelope.request.intent.slots
    job_type = slots["Job"].value
    # print("===== slots =====")
    # print(slots)
    # print("=====")
    # print("===== job =====")
    # print(slots["Job"].name)
    # print(slots["Job"].value)
    # print("=====")

    if (number_of_jobs > 0):
        for job in jobs:
            print("===== Job name=====")
            print(job['name'].lower())
            if (job_type.lower() in job['name'].lower()):
                stop_job = job
                print("===== Job to stop =====")
                print(job)
                print("=====")
                break

        if (stop_job != None):
            print("Ok, you have a total of " + str(number_of_jobs) + " active " + job_type + " jobs. Stopping the last modified job named " + stop_job['name'])
            speech_text = "Ok, you have a total of " + str(number_of_jobs) + " active " + job_type + " jobs. Stopping the last modified job named " + stop_job['name']
            r = requests.post(SCH_BASE_URL + '/jobrunner/rest/v1/job/' + stop_job['id'] + '/stop', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})
            print("===== Job requested to stop =====")
            print(json.loads(r.text))
            print("=====")
        else:
            print("You have a total of " + str(number_of_jobs) + " active jobs, but none of them are of type '" + job_type + ".'")
            speech_text = "You have a total of " + str(number_of_jobs) + " active jobs, but none of them are of type '" + job_type + ".'"
    else:
        speech_text = "You have a total of 0 active " + job_type + " jobs."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class PipelinesIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("PipelinesIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/pipelinestore/rest/v1/pipelines/count', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})

    speech_text = "You have a total of " + str(json.loads(r.text)['count']) + " pipelines."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class JobsIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("JobsIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/jobrunner/rest/v1/jobs/count', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})

    speech_text = "You have a total of " + str(json.loads(r.text)['count']) + " jobs."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class ActiveJobsIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("ActiveJobsIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/jobrunner/rest/v1/jobs/byStatus?jobStatus=ACTIVE', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})

    speech_text = "You have a total of " + str(len(json.loads(r.text))) + " active jobs."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class InactiveJobsIntentHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_intent_name("InactiveJobsIntent")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    # speech_text = "Hello StreamSets"

    r = requests.get(SCH_BASE_URL + '/jobrunner/rest/v1/jobs/byStatus?jobStatus=INACTIVE', headers={"X-SS-User-Auth-Token": auth_token, "X-Requested-By":"Alexa", "Content-Type": "application/json"})

    speech_text = "You have a total of " + str(len(json.loads(r.text))) + " inactive jobs."

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello StreamSets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class AllExceptionHandler(AbstractExceptionHandler):
    def can_handle(self, handler_input, exception):
        # type: (HandlerInput, Exception) -> bool
        return True

    def handle(self, handler_input, exception):
        # type: (HandlerInput, Exception) -> Response
        # Log the exception in CloudWatch Logs
        print(exception)

        speech = "Sorry, I didn't get it. Can you please say it again!"
        handler_input.response_builder.speak(speech).ask(speech)
        return handler_input.response_builder.response


class LaunchRequestHandler(AbstractRequestHandler):
  def can_handle(self, handler_input):
    # type: (HandlerInput) -> bool
    return is_request_type("LaunchRequest")(handler_input)

  def handle(self, handler_input):
    # type: (HandlerInput) -> Response
    speech_text = "Hello! Stream Sets Data Ops Platform at your service! How can I help you?"
    print(speech_text)

    handler_input.response_builder.speak(speech_text).set_card(
        SimpleCard("Hello Stream Sets", speech_text)).set_should_end_session(
        False)
    return handler_input.response_builder.response


class HelpIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        # type: (HandlerInput) -> bool
        return is_intent_name("AMAZON.HelpIntent")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        speech_text = "Hello! Stream Sets Data Ops Platform at your service! How can I help you?"
        print(speech_text)

        handler_input.response_builder.speak(speech_text).set_card(
            SimpleCard("Hello Stream Sets", speech_text)).set_should_end_session(
            False)
        return handler_input.response_builder.response


class CancelAndStopIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        # type: (HandlerInput) -> bool
        return is_intent_name("AMAZON.CancelIntent")(handler_input) or is_intent_name("AMAZON.StopIntent")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        speech_text = "Goodbye!"

        handler_input.response_builder.speak(speech_text).set_card(
            SimpleCard("Hello Stream Sets", speech_text)).set_should_end_session(True)
        return handler_input.response_builder.response


class SessionEndedRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        # type: (HandlerInput) -> bool
        return is_request_type("SessionEndedRequest")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        # any cleanup logic goes here

        return handler_input.response_builder.response


sb.add_request_handler(JobsIntentHandler())
sb.add_request_handler(StartJobIntentHandler())
sb.add_request_handler(StopJobIntentHandler())
sb.add_request_handler(ActiveJobsIntentHandler())
sb.add_request_handler(InactiveJobsIntentHandler())
sb.add_request_handler(PipelinesIntentHandler())

sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(HelpIntentHandler())
sb.add_request_handler(CancelAndStopIntentHandler())
sb.add_request_handler(SessionEndedRequestHandler())

sb.add_exception_handler(AllExceptionHandler())

handler = sb.lambda_handler()
