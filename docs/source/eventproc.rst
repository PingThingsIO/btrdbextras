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

Our platform will provide a base python 3.7.3 environment with which you can add any desired libraries installable with `pip`.  When registering an event handler you can provide the content for a requirements file as if you had run `pip freeze` at the command line.  We provide some standard libraries such as:

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

Flags
~~~~~~~~~~~~~~~~~~~

Each event handler also includes a `flags` argument.  This allows the user to choose which event handlers should run in any given scenarion.  For instance, if submitting new COMTRADE files to the ingress, you can choose the "voltage-transform" flag to indicate that any event handlers registered with this flag will be executed.  Multiple flags can be added to your event handler, and users can choose from multiple flags to determine which handlers should run.  In the latter case, a logical OR is used such that any handler with at least one of the chosen flags will be scheduled for execution.

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

The following calls can be made to platform to manage your event handling code.

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

    register(conn, name, hook, notify_on_success, notify_on_failure, dependencies, flags=None)
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
        dependencies: str
            A string containing required libraries and versions.  Format should follow
            a standard `pip freeze` or requirements.txt file.  The process running your
            handler will `pip install -r` this content.
        flags: list of str
            Filtering flags that users can choose when identifying handlers to
            execute.

As you can see, this decorator does have required arguments.  A trivial example is shown below.

.. code-block:: python

    >>> from btrdbextras import Connection
    >>> from btrdbextras.eventproc import register
    >>> conn = Connection()
    >>>
    >>> @register(
    ...     conn,
    ...     "trivial-handler",
    ...     "ctingress.on_complete",
    ...     "success@example.com",
    ...     "failure@example.com",
    ...     "scipy\nmatplotlib",
    ...     ["demo", "anomaly-detection"]
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
    [Handler(id=3, name='sample-73', hook='ctingress.on_complete', version=0, notify_on_success='success@example.com', notify_on_failure='failure@example.com', flags=['red', 'blue'], created_at=datetime.datetime(2020, 10, 21, 21, 35, 20, 365664, tzinfo=<UTC>), created_by='allen', updated_at=datetime.datetime(2020, 10, 21, 21, 35, 20, 365664, tzinfo=<UTC>), updated_by='allen')]

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
    [Handler(id=5, name='trivial-handler', hook='ctingress.on_complete', version=0, notify_on_success='success@example.com', notify_on_failure='failure@example.com', flags=['demo', 'anomaly-detection'], created_at=datetime.datetime(2020, 10, 23, 15, 6, 48, 390720, tzinfo=<UTC>), created_by='allen', updated_at=datetime.datetime(2020, 10, 23, 15, 6, 48, 390720, tzinfo=<UTC>), updated_by='allen')]
    >>>
    >>> deregister(conn, 5)
    True


Troubleshooting
--------------------------

At the moment there are only a few troubleshooting tips however we expect to add more content over time.

Most importantly, ensure you are using Python 3.7.3.  This is the version supplied by our Juupyter servers so you should be fine on this front unless you are making calls on your local machine.  In the future we plan on providing support for all versions of Python greater than 3.6.0.


Hooks
--------------------------

The available hooks are listed below.  Over time new hooks will be added to the platform.

ctingress.on_complete
~~~~~~~~~~~~~~~~~~~~~

Event handlers for this hook will be executed after processing an entire archive of COMTRADE files.  The signature for your event handler should match the following arguments:

handler(btrdb, uuids):
    A function that is executed after the COMTRADE ingress processes a new
    archive containing COMTRADE data.

    Parameters
    ----------
    btrdb
        A Python btrdb connection object for querying the time series database.
    uuids
        A list of UUIDs (as string) representing streams that were involved in the
        data import.