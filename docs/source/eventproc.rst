Event Processing
=========================

Overview
--------

The Event Processing library allows you to run arbitrary code (event handlers) at predefined events (also called hooks) within the PredictiveGrid platform.  A sample use case might be if you would like to upload new COMTRADE files using the COMTRADE ingress and have a transform or analytical process run immediately after import.

Notifications
~~~~~~~~~~~~~~~~~~~

After your event handler is executed, an email is sent to the success or failure email address supplied during registration.  If successful, the email will contain any text that was returned by your Python function.  If an exception is raised, then the handler code is regarded as unsuccesful and a notification is sent to the corresponding failure email address with exception details.

Dependencies
~~~~~~~~~~~~~~~~~~~

Our platform will provide a base python 3.7.3 environment similar to our Jupyter servers. We provide some standard libraries such as:

* btrdb
* matplotlib
* numpy
* pandas
* ray
* scikit-learn
* scipy
* statsmodels
* tensorflow
* torch
* torchvision

Tags
~~~~~~~~~~~~~~~~~~~

Each event handler also includes a `tags` argument.  This allows the user to choose which event handlers should run in any given scenarion.  For instance, if submitting new COMTRADE files to the ingress, you can choose the "voltage-transform" tag to indicate that any event handlers registered with this tag will be executed.  Multiple tags can be added to your event handler, and users can choose from multiple tags to determine which handlers should run.  In the latter case, a logical OR is used such that any handler with at least one of the chosen tags will be scheduled for execution.

Connections
-----------

Each of the following calls require a `Connection` object (different from the btrdb library connection object) as the first argument.  When registering a new event handler, the API key in the `Connection` object will be used as the execution time security context - meaning all code will be executed as the registering user.

The Connection object requires the BTrDB address/port as well as an API key as follows.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> conn = Connection("api.example.com:4411", "C27489F2BFACE794A3")


However, the `Connection` object will also look for the `BTRDB_ENDPOINTS` and `BTRDB_API_KEY` environment variables if no arguments are supplied.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> conn = Connection()


Interactions
------------

The following calls can be made to the platform to manage your event handling code.

Listing Known Hooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can think of hooks as just labels that represent a specific point during the execution of some process (such as importing data).  In order to see the allowed hooks, you may invoke the `hooks` function.  Over time, new `hooks` will be added to the platform allowing you to integrate your own code into different processes.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> from btrdbextras.eventproc import hooks

    >>> conn = Connection()
    >>> hooks(conn)
    ['ctingress.on_complete']


Registering Event Handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The event handlers are supplied by you.  This is code you would like executed when a specific hook/event has occurred within the larger platform. Your Python callable, typically just a function, will be pickled using the `dill` library for execution in the platform.  As such there are certain Python code serialization topics to keep in mind such as: total size of the callable, libraries available in the execution environment, etc.

To submit a new event handler, you can use the `register` decorator around your own Python callable.  Like all objects in this library, you can use the `help` function to view the docstring.

.. code-block:: python

    >>> from btrdbextras.eventproc import register
    >>> help(register)
    Help on function register in module btrdbextras.eventproc.eventproc:

    register(conn, name, hook, notify_on_success, notify_on_failure, tags=None)
        decorator to submit (register) an event handler function

        Parameters
        ----------
        conn: Connection
            btrdbextras Connection object containing a valid address and api key.
        name: str
            Friendly name of this event handler for display purposes.
        hook: str
            Name of the hook that this event handler responds to.
        notify_on_success: str
            Email address of user to notify when event handler completes successfully.
        notify_on_failure: str
            Email address of user to notify when event handler does not complete
            successfully.
        tags: list of str
            Filtering tags that users can choose when identifying handlers to
            execute. An empty list will match all tags.

As you can see, this decorator does have required arguments.  A trivial example is shown below.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> from btrdbextras.eventproc import register
    >>> conn = Connection()
    >>>
    >>> @register(
    ...     conn=conn,
    ...     name="trivial-handler",
    ...     hook="ctingress.on_complete",
    ...     notify_on_success="success@example.com",
    ...     notify_on_failure="failure@example.com",
    ...     tags=["demo", "anomaly-detection"]
    ... )
    ... def trivial(btrdb, *args, **kwargs):
    ...     print(args, kwargs)
    ...     return "completed successfully"

The library will also allow you to update an existing event handler using the register decorator.  No extra steps are needed, just register the new version with the same hook and name, and the old one will get replaced.


Listing Registered Handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To view the existing event handlers, call the `list_handlers` function and a list of `Handler` objects will be returned.  You may also provide an optional `hooks` argument in order to filter the results to only the hook/event you are interested in.


.. code-block:: python

    >>> from btrdbextras import Connection
    >>> from btrdbextras.eventproc import list_handlers
    >>> conn = Connection()
    >>>
    >>> list_handlers(conn)
    [Handler(id=3, name='sample-73', hook='ctingress.on_complete', version=0, notify_on_success='success@example.com', notify_on_failure='failure@example.com', tags=['red', 'blue'], created_at=datetime.datetime(2020, 10, 21, 21, 35, 20, 365664, tzinfo=<UTC>), created_by='allen', updated_at=datetime.datetime(2020, 10, 21, 21, 35, 20, 365664, tzinfo=<UTC>), updated_by='allen')]

The `Handler` object is just a lightweight `namedtuple` and does not offer any functionality itself.

Deleting Existing Handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The remove an existing event handler, a `deregister` function is available.  It requires only the ID of your handler which was determined when you first registered.  You can use `list_handlers` to find the ID for your handler if needed.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> from btrdbextras.eventproc import list_handlers, deregister
    >>> conn = Connection()
    >>>
    >>> list_handlers(conn)
    [Handler(id=5, name='trivial-handler', hook='ctingress.on_complete', version=0, notify_on_success='success@example.com', notify_on_failure='failure@example.com', tags=['demo', 'anomaly-detection'], created_at=datetime.datetime(2020, 10, 23, 15, 6, 48, 390720, tzinfo=<UTC>), created_by='allen', updated_at=datetime.datetime(2020, 10, 23, 15, 6, 48, 390720, tzinfo=<UTC>), updated_by='allen')]
    >>>
    >>> deregister(conn, 5)
    True


Writing Handlers
--------------------------

Code executing in a handler can import and use packages and read/write files, similarly to how the handler function works in the environment you defined/registered the handler in.

Using Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any packages used within a handler must be imported within that handler as follows.

.. code-block:: python

    >>> def import_handler(*args, **kwargs):
    ...     import pandas as pd
    ...     df = pd.DataFrame([['a', 'b'], ['c', 'd']],
                            index=['row 1', 'row 2'],
                            columns=['col 1', 'col 2'])
    ...     return "done"

The exception to this is the `btrdbextras.eventproc` module, which is already imported as `ep` in the handler's execution environment. As a result, both of the following uses of the `eventproc` module are valid.

.. code-block:: python

    >>> def upload_handler_v1(*args, **kwargs):
    ...     from btrdbextras.eventproc import upload_file
    ...     path = "demo_filepath"
    ...     f = open(path, "w")
    ...     f.write("hello world!")
    ...     f.close()
    ...     url = upload_file(path, name_on_download)
    ...     return url
    >>>
    >>> def upload_handler_v2(*args, **kwargs):
    ...     path = "demo_filepath"
    ...     f = open(path, "w")
    ...     f.write("hello world!")
    ...     f.close()
    ...     url = ep.upload_file(path, name_on_download)
    ...     return url


Working with Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any file created by a handler will be destroyed after the handler completes, and will not persist in the handler's execution environment. To access files written by a handler outside of the handler's execution context, use the `upload_file` function. For example, the following code registers a handler that creates and uploads an excel file. The return value is a link to download the file (username and password authentication will be necessary for file download).

.. code-block:: python

    >>> @register(
    ...     conn=conn,
    ...     name="excel-demo",
    ...     hook="ctingress.on_complete",
    ...     notify_on_success="success@example.com",
    ...     notify_on_failure="failure@example.com",
    ...     tags=["excel-demo"]
    ...     )
    >>>
    >>> def upload_handler(*args, **kwargs):
    ...     import pandas as pd
    ...     path = "output.xlsx"
    ...     name_on_download = "excel_test.xlsx"
    ...     df = pd.DataFrame([['a', 'b'], ['c', 'd']],
                            index=['row 1', 'row 2'],
                            columns=['col 1', 'col 2'])
    ...     df.to_excel(path)
    ...     url = ep.upload_file(path, name_on_download)
    ...     return url


Troubleshooting
--------------------------

At the moment there are only a few troubleshooting tips however we expect to add more content over time.

Most importantly, ensure you are using Python 3.7.3.  This is the version supplied by our Jupyter servers so you should be fine on this front unless you are making calls on your local machine.  In the future we plan on providing support for all versions of Python greater than 3.6.0.


Hooks
--------------------------

The available hooks are listed below.  Over time new hooks will be added to the platform.

ctingress.on_complete
~~~~~~~~~~~~~~~~~~~~~

Event handlers for this hook will be executed after processing an entire archive of COMTRADE files.  The signature for your event handler should match the following arguments:

handler(btrdb, DataSets, File, RequestID, SubmittedAt, **kwargs):
    This function is executed after the COMTRADE ingress processes a new archive containing COMTRADE data.  All inputs are sent as keyword arguments.

    Implementers will find the 'DataSets' parameter to be the most useful as it will contain a list of dictionaries representing the status of each COMTRADE config/data file pairing.  If an error is encountered this will be displayed in the 'Error' key.  Similarly, if no issues were encountered the 'Success' key will equal True.  The 'Name' key represents the config file path within the archive.  The 'Streams' key contians a list of dictionaries representing individual streams within the COMTRADE file. Key/values for stream status include the start and end time of the included data (StartNs, EndNs), the time ingress began processing the stream (ProcessedAtNs), and the UUID of the stream.

    While listed in documentation, **kwargs is not needed at this time though further arguments may be added in the future.

    Parameters
    ----------
    btrdb
        A Python btrdb connection object for querying the time series database.
    DataSets
        A list of dictionaries representing the processing status of each COMTRADE config/data file pair.
    File
        The name of the uploaded archive file containing COMTRADE file pairs.
    RequestID
        A unique identifier for this submission.
    SubmittedAt
        The date/time for this submission.

Assuming you chose to use variadic arguments (i.e., `**kwargs`) in your event handler, the input would look similar to the following.  This sample shows an archive file that included one pair of config/data COMTRADE files along with an erroneous file to show what an error would look like.

.. code-block:: python

    {
        'DataSets': [
            {
                'Error': '',
                'Name': '/DFR 2/200610,230000000,-4t,R108-North Anna #1,APP601,Dominion,M1094.cfg',
                'Streams': [{'EndNs': 1591844539983333333,
                            'ProcessedAtNs': 0,
                            'StartNs': 1591844350000000000,
                            'Uuid': '585cc764-3c80-5d3f-a7d7-59d48abbab0b'},
                            {'EndNs': 1591844539983333333,
                            'ProcessedAtNs': 0,
                            'StartNs': 1591844350000000000,
                            'Uuid': 'ee614a71-803a-511e-bbb7-6fb84210228d'},
                            {'EndNs': 1591844539983333333,
                            'ProcessedAtNs': 0,
                            'StartNs': 1591844350000000000,
                            'Uuid': '6575e59b-e486-5630-8596-9ef657f65a99'},
                            {'EndNs': 1591844539983333333,
                            'ProcessedAtNs': 0,
                            'StartNs': 1591844350000000000,
                            'Uuid': '801674d5-d6eb-5031-84f6-8746dce0761c'},
                            {'EndNs': 1591844539983333333,
                            'ProcessedAtNs': 0,
                            'StartNs': 1591844350000000000,
                            'Uuid': '597f8584-2acb-5a92-85bd-0002733fb920'}],
                'Success': True
            },
            {
                'Error': 'cfg format error',
                'Name': '/__MACOSX/DFR 2/._200610,230000000,-4t,R108-North Anna #1,APP601,Dominion,M1094.cfg',
                'Streams': None,
                'Success': False
            }
        ],
        'File': 'comtrade_samples.zip',
        'RequestID': '0bbcacdb-e7aa-4e01-af88-9ba865ea32bd',
        'SubmittedAt': '2020-10-29T21:58:15.09246084Z'
    }