'use strict';
var React = require('react');
var cloneWithProps = require('react/lib/cloneWithProps');


var Panel = module.exports.Panel = React.createClass({
    propTypes: {
        addClasses: React.PropTypes.string // Classes to add to outer panel div
    },

    render: function() {
        return (
            <div className={'panel panel-default' + (this.props.addClasses ? ' ' + this.props.addClasses : '')}>
                {this.props.children}
            </div>
        );
    }
});


var PanelBody = module.exports.PanelBody = React.createClass({
    propTypes: {
        addClasses: React.PropTypes.string // Classes to add to outer panel div
    },

    render: function() {
        return (
            <div className={'panel-body' + (this.props.addClasses ? ' ' + this.props.addClasses : '')}>
                {this.props.children}
            </div>
        );
    }
});


var PanelHeading = module.exports.PanelHeading = React.createClass({
    propTypes: {
        addClasses: React.PropTypes.string // Classes to add to outer panel div
    },

    render: function() {
        return (
            <div className={'panel-heading' + (this.props.addClasses ? ' ' + this.props.addClasses : '')}>
                {this.props.children}
            </div>
        );
    }
});


// <TabPanel> components have tabs that select between panes, and so the main child components of
// <TabPanel> must be <TabPanelPane> components. The children of <TabPanelPane> components are the
// content you want to have rendered within a tab's pane. <TabPanel> takes a required `tabs`
// parameter -- an object that maps an identifier to a tab title. The identifier has to map to a
// child <TabPanelPane> `key` property. Here's an example tabbed panel.
//
// <TabPanel tabs={{panel1: 'Panel 1', panel2: 'Panel 2', panel3: 'Panel 3'}}>
//     <TabPanelPane key="panel1">
//         <div>Content for panel 1</div>
//     </TabPanelPane>
//     <TabPanelPane key="panel2">
//         <div>Content for panel 2</div>
//     </TabPanelPane>
//     <TabPanelPane key="panel3">
//         <div>Content for panel 3</div>
//     </TabPanelPane>
// </TabPanel>
//
// Note that <TabPanelPane> takes an `id` property, not a `key` property because components can't
// receive those. <TabPanel> copies the `key` property to an `id` property in any child <TabPanel>
// components so that <TabPanel> can see it.

var TabPanel = module.exports.TabPanel = React.createClass({
    propTypes: {
        tabs: React.PropTypes.object.isRequired // Object with tab=>pane specifications
    },

    getInitialState: function() {
        return {currentTab: ''};
    },

    // Handle a click on a tab
    handleClick: function(tab) {
        if (tab !== this.state.currentTab) {
            this.setState({currentTab: tab});
        }
    },

    render: function() {
        var children = [];
        var tabs = this.props.tabs;

        // We expect to find <TabPanelPane> child elements inside <TabPanel>. For any we find, get
        // the React `key` value and copy it to an `id` value that we add to each child component.
        // That lets each child get an HTML ID matching `key` without having to pass both a key and
        // id with the same value. We also set the `active` property in the TabPanelPane component
        // here too so that each pane knows whether it's the active one or not. ### React14
        if (this.props.children) {
            children = React.Children.map(this.props.children, (child, i) => {
                if (child.type === TabPanelPane.type) {
                    // Replace the existing child <TabPanelPane> component
                    return cloneWithProps(child, {id: child.key, active: this.state.currentTab ? this.state.currentTab === child.key : i === 0});
                }
                return child;
            });
        }

        return (
            <div>
                <ul className="nav nav-tabs" role="tablist">
                    {Object.keys(tabs).map((tab, i) => {
                        var currentTab = (i === 0 && this.state.currentTab === '') ? tab : this.state.currentTab;

                        return (
                            <li key={tab} role="presentation" aria-controls={tab} className={currentTab === tab ? 'active' : ''}>
                                <a href={'#' + tab} ref={tab} onClick={this.handleClick.bind(this, tab)} data-trigger="tab" aria-controls={tab} role="tab" data-toggle="tab">{tabs[tab]}</a>
                            </li>
                        );
                    })}
                </ul>
                <div className="tab-content">
                    {children}
                </div>
            </div>
        );
    }
});


var TabPanelPane = module.exports.TabPanelPane = React.createClass({
    propTypes: {
        id: React.PropTypes.string.isRequired, // ID of the pane; not passed explicitly -- comes from `key` of <TabPanelPane>
        active: React.PropTypes.bool // True if this panel is the active one
    },

    render: function() {
        return (
            <div role="tabpanel" className={'tab-pane' + (this.props.active ? ' active' : '')} id={this.props.id}>
                {this.props.children}
            </div>
        );
    }
});
